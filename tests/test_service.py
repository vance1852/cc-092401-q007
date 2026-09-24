from __future__ import annotations

import json
import sqlite3
import unittest
from datetime import datetime, timezone
from pathlib import Path

from robot_trials.clock import FrozenClock
from robot_trials.errors import Conflict, Forbidden, InvalidState
from robot_trials.jsonio import load_json
from robot_trials.service import TrialService


ROOT = Path(__file__).resolve().parents[1]


class ServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:", isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.clock = FrozenClock(datetime(2026, 9, 24, 8, 0, tzinfo=timezone.utc))
        self.service = TrialService(self.connection, self.clock)
        for user_id, role in (
            ("operator", "operator"),
            ("stat", "statistician"),
            ("approver", "approver"),
            ("auditor", "auditor"),
        ):
            self.service.create_user(user_id, user_id, role)
        self.protocol = load_json(ROOT / "fixtures" / "demo_protocol.json")
        self.rows = [
            json.loads(line)
            for line in (ROOT / "fixtures" / "demo_observations.jsonl").read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        self.service.register_robot("operator", "robot-a", "A 型", "厂商")
        self.service.register_build("operator", "build-a", "robot-a", "1.0", "b" * 64)
        self.service.publish_protocol("stat", self.protocol)
        self.service.create_batch("operator", "batch-a", "demo-delivery-v1", 1, "build-a")
        self.service.start_batch("operator", "batch-a", 1)

    def tearDown(self) -> None:
        self.connection.close()

    def test_complete_workflow(self) -> None:
        imported = self.service.import_observations("operator", "batch-a", "key-1", self.rows)
        self.assertEqual(imported["inserted"], 6)
        self.service.seal_batch("stat", "batch-a", 2)
        job = self.service.claim_job("worker", 30)
        analysis = self.service.complete_job("worker", job["job_id"], "stat")
        self.service.decide("approver", "batch-a", analysis["analysis_id"], "approved", "满足规则")
        report = self.service.report("auditor", "batch-a")
        self.assertEqual(report["batch"]["state"], "decided")
        self.assertEqual(report["analysis"]["result"]["conclusion"], "pass")

    def test_idempotent_replay_and_conflict(self) -> None:
        first = self.service.import_observations("operator", "batch-a", "key-1", self.rows)
        second = self.service.import_observations("operator", "batch-a", "key-1", self.rows)
        self.assertEqual(first, second)
        changed = [dict(item) for item in self.rows]
        changed[0] = dict(changed[0])
        changed[0]["metrics"] = dict(changed[0]["metrics"])
        changed[0]["metrics"]["completion_seconds"] = "99"
        with self.assertRaises(Conflict):
            self.service.import_observations("operator", "batch-a", "key-1", changed)
        count = self.connection.execute("SELECT count(*) FROM observations").fetchone()[0]
        self.assertEqual(count, 6)

    def test_import_rolls_back_when_one_source_row_duplicates(self) -> None:
        self.service.import_observations("operator", "batch-a", "key-1", self.rows[:1])
        with self.assertRaises(Conflict):
            self.service.import_observations("operator", "batch-a", "key-2", self.rows[:2])
        count = self.connection.execute("SELECT count(*) FROM observations").fetchone()[0]
        self.assertEqual(count, 1)

    def test_role_separation(self) -> None:
        with self.assertRaises(Forbidden):
            self.service.seal_batch("operator", "batch-a", 2)
        with self.assertRaises(Forbidden):
            self.service.report("operator", "batch-a")

    def test_exclusion_review_and_revoke_leave_history(self) -> None:
        self.service.import_observations("operator", "batch-a", "key-1", self.rows)
        observation_id = self.connection.execute(
            "SELECT observation_id FROM observations ORDER BY observation_id LIMIT 1"
        ).fetchone()[0]
        requested = self.service.request_exclusion("operator", observation_id, "现场记录失效")
        reviewed = self.service.review_exclusion("stat", requested["exclusion_id"], True, "证据充分")
        self.assertEqual(reviewed["status"], "approved")
        revoked = self.service.revoke_exclusion("operator", requested["exclusion_id"], "已找回原始记录")
        self.assertEqual(revoked["status"], "revoked")
        events = self.connection.execute(
            "SELECT event_type FROM audit_events WHERE entity_type='observation' AND entity_id=? ORDER BY event_id",
            (str(observation_id),),
        ).fetchall()
        self.assertEqual([row[0] for row in events], ["exclusion.requested", "exclusion.revoked"])

    def _expected_batch_event_ids(self, batch_id: str) -> list[int]:
        """直接从底层审计表按归属关系计算某批次应有的事件 ID 链。"""

        rows = self.connection.execute(
            """
            SELECT a.event_id FROM audit_events a
            LEFT JOIN observations ao
                ON a.entity_type='observation' AND ao.observation_id=CAST(a.entity_id AS INTEGER)
            LEFT JOIN exclusion_requests ae
                ON a.entity_type='exclusion' AND ae.exclusion_id=CAST(a.entity_id AS INTEGER)
            LEFT JOIN observations aeo ON aeo.observation_id=ae.observation_id
            WHERE
                (a.entity_type='batch' AND a.entity_id=:batch)
                OR (a.entity_type='observation'
                    AND COALESCE(json_extract(a.payload_json, '$.batch_id'), ao.batch_id)=:batch)
                OR (a.entity_type='exclusion'
                    AND COALESCE(json_extract(a.payload_json, '$.batch_id'), aeo.batch_id)=:batch)
            ORDER BY a.event_id
            """,
            {"batch": batch_id},
        ).fetchall()
        return [row[0] for row in rows]

    def _setup_second_batch(self) -> None:
        self.service.create_batch("operator", "batch-b", "demo-delivery-v1", 1, "build-a")
        self.service.start_batch("operator", "batch-b", 1)
        self.service.import_observations("operator", "batch-b", "key-b-1", self.rows)

    def test_report_events_cover_full_approve_then_revoke_chain(self) -> None:
        self.service.import_observations("operator", "batch-a", "key-1", self.rows)
        observation_id = self.connection.execute(
            "SELECT observation_id FROM observations ORDER BY observation_id LIMIT 1"
        ).fetchone()[0]
        requested = self.service.request_exclusion("operator", observation_id, "现场记录失效")
        self.service.review_exclusion("stat", requested["exclusion_id"], True, "证据充分")
        self.service.revoke_exclusion("operator", requested["exclusion_id"], "已找回原始记录")

        report = self.service.report("auditor", "batch-a")
        events = report["events"]

        expected_ids = self._expected_batch_event_ids("batch-a")
        self.assertEqual([event["event_id"] for event in events], expected_ids)
        self.assertEqual(
            [event["event_type"] for event in events],
            ["batch.created", "batch.started", "observations.imported",
             "exclusion.requested", "exclusion.approved", "exclusion.revoked"],
        )
        self.assertEqual(
            {(event["entity_type"], event["entity_id"]) for event in events if event["event_type"].startswith("exclusion.")},
            {
                ("observation", str(observation_id)),
                ("exclusion", str(requested["exclusion_id"])),
                ("observation", str(observation_id)),
            },
        )
        for event in events:
            self.assertIn("entity_type", event)
            self.assertIn("entity_id", event)
            self.assertIn("payload", event)
            self.assertIn("created_at", event)
            self.assertIn("actor_id", event)

    def test_report_events_cover_rejected_exclusion(self) -> None:
        self.service.import_observations("operator", "batch-a", "key-1", self.rows)
        observation_id = self.connection.execute(
            "SELECT observation_id FROM observations ORDER BY observation_id LIMIT 1"
        ).fetchone()[0]
        requested = self.service.request_exclusion("operator", observation_id, "疑似异常")
        self.service.review_exclusion("stat", requested["exclusion_id"], False, "证据不足")

        events = self.service.report("auditor", "batch-a")["events"]
        self.assertEqual(
            [event["event_type"] for event in events][-2:],
            ["exclusion.requested", "exclusion.rejected"],
        )
        self.assertEqual(events[-1]["entity_type"], "exclusion")
        self.assertEqual(events[-1]["entity_id"], str(requested["exclusion_id"]))
        self.assertEqual(
            [event["event_id"] for event in events], self._expected_batch_event_ids("batch-a")
        )

    def test_report_events_never_mix_other_batches(self) -> None:
        self._setup_second_batch()
        self.service.import_observations("operator", "batch-a", "key-1", self.rows)

        obs_a = self.connection.execute(
            "SELECT observation_id FROM observations WHERE batch_id='batch-a' ORDER BY observation_id LIMIT 1"
        ).fetchone()[0]
        obs_b = self.connection.execute(
            "SELECT observation_id FROM observations WHERE batch_id='batch-b' ORDER BY observation_id LIMIT 1"
        ).fetchone()[0]
        req_a = self.service.request_exclusion("operator", obs_a, "批次 A 的排除申请")
        req_b = self.service.request_exclusion("operator", obs_b, "批次 B 的排除申请")
        self.service.review_exclusion("stat", req_a["exclusion_id"], True, "A 批准")
        self.service.review_exclusion("stat", req_b["exclusion_id"], False, "B 驳回")

        report_a = self.service.report("auditor", "batch-a")
        report_b = self.service.report("auditor", "batch-b")

        ids_a = [event["event_id"] for event in report_a["events"]]
        ids_b = [event["event_id"] for event in report_b["events"]]
        self.assertEqual(ids_a, self._expected_batch_event_ids("batch-a"))
        self.assertEqual(ids_b, self._expected_batch_event_ids("batch-b"))
        self.assertTrue(set(ids_a).isdisjoint(ids_b))

        chain_a = [(event["entity_type"], event["event_type"]) for event in report_a["events"]]
        chain_b = [(event["entity_type"], event["event_type"]) for event in report_b["events"]]
        self.assertIn(("observation", "exclusion.requested"), chain_a)
        self.assertIn(("exclusion", "exclusion.approved"), chain_a)
        self.assertNotIn(("exclusion", "exclusion.rejected"), chain_a)
        self.assertIn(("exclusion", "exclusion.rejected"), chain_b)
        self.assertNotIn(("exclusion", "exclusion.approved"), chain_b)

        referenced = {
            (event["entity_type"], event["entity_id"]) for event in report_a["events"]
        }
        self.assertNotIn(("observation", str(obs_b)), referenced)
        self.assertNotIn(("exclusion", str(req_b["exclusion_id"])), referenced)
        self.assertNotIn(("batch", "batch-b"), referenced)

    def test_legacy_audit_events_without_batch_id_are_attributed_via_relations(self) -> None:
        self.service.import_observations("operator", "batch-a", "key-1", self.rows)
        self._setup_second_batch()
        obs_a = self.connection.execute(
            "SELECT observation_id FROM observations WHERE batch_id='batch-a' ORDER BY observation_id LIMIT 1"
        ).fetchone()[0]
        obs_b = self.connection.execute(
            "SELECT observation_id FROM observations WHERE batch_id='batch-b' ORDER BY observation_id LIMIT 1"
        ).fetchone()[0]

        # 直接写入旧格式审计事件：payload 中没有 batch_id，只能依赖关系表归属。
        cursor = self.connection.execute(
            "INSERT INTO exclusion_requests(observation_id,status,reason,requested_by,requested_at) "
            "VALUES(?,?,?,?,?)",
            (obs_a, "approved", "旧数据申请", "operator", "2026-09-24T07:00:00Z"),
        )
        legacy_exclusion_a = cursor.lastrowid
        self.connection.execute(
            "INSERT INTO audit_events(entity_type,entity_id,event_type,actor_id,payload_json,created_at) "
            "VALUES('observation',?,'exclusion.requested','operator',?,?)",
            (str(obs_a), '{"reason":"旧数据申请"}', "2026-09-24T07:00:01Z"),
        )
        self.connection.execute(
            "INSERT INTO audit_events(entity_type,entity_id,event_type,actor_id,payload_json,created_at) "
            "VALUES('exclusion',?,'exclusion.approved','stat',?,?)",
            (str(legacy_exclusion_a), '{"note":"旧数据批准"}', "2026-09-24T07:00:02Z"),
        )
        # 另一批次的同类旧事件必须被排除在 batch-a 报告之外。
        self.connection.execute(
            "INSERT INTO audit_events(entity_type,entity_id,event_type,actor_id,payload_json,created_at) "
            "VALUES('observation',?,'exclusion.requested','operator',?,?)",
            (str(obs_b), '{"reason":"其他批次申请"}', "2026-09-24T07:00:03Z"),
        )

        events = self.service.report("auditor", "batch-a")["events"]
        self.assertEqual(
            [event["event_type"] for event in events][-2:],
            ["exclusion.requested", "exclusion.approved"],
        )
        self.assertEqual(
            [event["event_id"] for event in events], self._expected_batch_event_ids("batch-a")
        )
        self.assertNotIn("其他批次申请", [event["payload"].get("reason") for event in events])

        events_b = self.service.report("auditor", "batch-b")["events"]
        self.assertIn(
            "其他批次申请", [event["payload"].get("reason") for event in events_b]
        )

    def test_report_events_are_sorted_by_global_event_id(self) -> None:
        self.service.import_observations("operator", "batch-a", "key-1", self.rows)
        observation_id = self.connection.execute(
            "SELECT observation_id FROM observations ORDER BY observation_id LIMIT 1"
        ).fetchone()[0]
        requested = self.service.request_exclusion("operator", observation_id, "申请")
        self.service.review_exclusion("stat", requested["exclusion_id"], True, "批准")

        event_ids = [event["event_id"] for event in self.service.report("auditor", "batch-a")["events"]]
        self.assertEqual(event_ids, sorted(event_ids))
        self.assertEqual(len(event_ids), len(set(event_ids)))

        self.service.import_observations("operator", "batch-a", "key-1", self.rows)
        self.service.seal_batch("stat", "batch-a", 2)
        job = self.service.claim_job("worker-a", 10)
        failed = self.service.fail_job("worker-a", job["job_id"], "临时计算失败", retry_seconds=5)
        self.assertEqual(failed["state"], "queued")
        self.assertIsNone(self.service.claim_job("worker-b", 10))
        self.clock.advance(seconds=5)
        retried = self.service.claim_job("worker-b", 10)
        self.assertEqual(retried["job_id"], job["job_id"])
        self.assertEqual(retried["attempts"], 2)

    def test_lease_can_be_reclaimed_after_expiry(self) -> None:
        self.service.import_observations("operator", "batch-a", "key-1", self.rows)
        self.service.seal_batch("stat", "batch-a", 2)
        first = self.service.claim_job("worker-a", 10)
        self.clock.advance(seconds=11)
        second = self.service.claim_job("worker-b", 10)
        self.assertEqual(first["job_id"], second["job_id"])
        self.assertEqual(second["lease_owner"], "worker-b")
        with self.assertRaises(InvalidState):
            self.service.complete_job("worker-a", first["job_id"], "stat")


if __name__ == "__main__":
    unittest.main()
