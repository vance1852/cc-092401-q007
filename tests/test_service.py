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

    def _create_second_batch(self, batch_id: str = "batch-b") -> None:
        self.service.register_build("operator", "build-b", "robot-a", "2.0", "c" * 64)
        self.service.create_batch("operator", batch_id, "demo-delivery-v1", 1, "build-b")
        self.service.start_batch("operator", batch_id, 1)
        self.service.import_observations("operator", batch_id, "key-b", self.rows)

    def _expected_event_ids(self, batch_id: str) -> list[int]:
        """按修复后的归属关系，从底层审计表推导该批次应有的事件链。"""
        rows = self.connection.execute(
            """
            SELECT a.event_id FROM audit_events a
            WHERE (a.entity_type='batch' AND a.entity_id=?)
               OR (a.entity_type='observation' AND a.entity_id IN (
                    SELECT CAST(observation_id AS TEXT) FROM observations WHERE batch_id=?))
               OR (a.entity_type='exclusion' AND a.entity_id IN (
                    SELECT CAST(e.exclusion_id AS TEXT) FROM exclusion_requests e
                    JOIN observations o ON o.observation_id=e.observation_id WHERE o.batch_id=?))
            ORDER BY a.event_id
            """,
            (batch_id, batch_id, batch_id),
        ).fetchall()
        return [row[0] for row in rows]

    def test_report_restores_full_exclusion_chain_with_entity_identity(self) -> None:
        self.service.import_observations("operator", "batch-a", "key-1", self.rows)
        observation_id = self.connection.execute(
            "SELECT observation_id FROM observations WHERE batch_id='batch-a' ORDER BY observation_id LIMIT 1"
        ).fetchone()[0]
        exclusion_id = self.service.request_exclusion("operator", observation_id, "现场记录失效")["exclusion_id"]
        self.service.review_exclusion("stat", exclusion_id, True, "证据充分")
        self.service.revoke_exclusion("operator", exclusion_id, "已找回原始记录")

        report = self.service.report("auditor", "batch-a")
        exclusion_events = [
            event for event in report["events"] if event["event_type"].startswith("exclusion.")
        ]
        self.assertEqual(
            [(event["event_type"], event["entity_type"], event["entity_id"]) for event in exclusion_events],
            [
                ("exclusion.requested", "observation", str(observation_id)),
                ("exclusion.approved", "exclusion", str(exclusion_id)),
                ("exclusion.revoked", "observation", str(observation_id)),
            ],
        )
        # 报告事件与底层审计表按关系归属出的事件完全一致，且按全局 event_id 升序。
        event_ids = [event["event_id"] for event in report["events"]]
        self.assertEqual(event_ids, self._expected_event_ids("batch-a"))
        self.assertEqual(event_ids, sorted(event_ids))
        self.assertTrue(all({"entity_type", "entity_id"} <= event.keys() for event in report["events"]))

    def test_report_includes_rejected_exclusion(self) -> None:
        self.service.import_observations("operator", "batch-a", "key-1", self.rows)
        observation_id = self.connection.execute(
            "SELECT observation_id FROM observations WHERE batch_id='batch-a' ORDER BY observation_id LIMIT 1"
        ).fetchone()[0]
        exclusion_id = self.service.request_exclusion("operator", observation_id, "疑似异常")["exclusion_id"]
        self.service.review_exclusion("stat", exclusion_id, False, "证据不足")

        report = self.service.report("auditor", "batch-a")
        self.assertEqual(
            [(event["event_type"], event["entity_type"]) for event in report["events"]
             if event["event_type"].startswith("exclusion.")],
            [("exclusion.requested", "observation"), ("exclusion.rejected", "exclusion")],
        )
        self.assertEqual(
            [event["event_id"] for event in report["events"]], self._expected_event_ids("batch-a")
        )

    def test_multi_batch_reports_do_not_mix_event_chains(self) -> None:
        self.service.import_observations("operator", "batch-a", "key-1", self.rows)
        self._create_second_batch()

        # 在两个批次交错制造排除事件，全局 event_id 交织，但归属必须彼此隔离。
        obs_a = self.connection.execute(
            "SELECT observation_id FROM observations WHERE batch_id='batch-a' ORDER BY observation_id LIMIT 1"
        ).fetchone()[0]
        obs_b = self.connection.execute(
            "SELECT observation_id FROM observations WHERE batch_id='batch-b' ORDER BY observation_id LIMIT 1"
        ).fetchone()[0]
        exc_a = self.service.request_exclusion("operator", obs_a, "A 批失效")["exclusion_id"]
        exc_b = self.service.request_exclusion("operator", obs_b, "B 批失效")["exclusion_id"]
        self.service.review_exclusion("stat", exc_a, True, "A 批准")
        self.service.review_exclusion("stat", exc_b, False, "B 驳回")
        self.service.revoke_exclusion("operator", exc_a, "A 撤销")

        report_a = self.service.report("auditor", "batch-a")
        report_b = self.service.report("auditor", "batch-b")
        ids_a = [event["event_id"] for event in report_a["events"]]
        ids_b = [event["event_id"] for event in report_b["events"]]

        # 数量、先后关系、归属与底层审计表完全一致；两批次事件链互不重叠。
        self.assertEqual(ids_a, self._expected_event_ids("batch-a"))
        self.assertEqual(ids_b, self._expected_event_ids("batch-b"))
        self.assertEqual(set(ids_a).intersection(ids_b), set())
        self.assertNotIn(str(exc_b), [event["entity_id"] for event in report_a["events"]])
        self.assertNotIn(str(obs_b), [event["entity_id"] for event in report_a["events"]])
        self.assertNotIn(str(exc_a), [event["entity_id"] for event in report_b["events"]])
        # B 批次只看到自己的驳回，看不到 A 批次的批准与撤销。
        b_exclusion_types = [
            event["event_type"] for event in report_b["events"]
            if event["event_type"].startswith("exclusion.")
        ]
        self.assertEqual(b_exclusion_types, ["exclusion.requested", "exclusion.rejected"])
        # 每个事件都携带实体类型与实体标识。
        for report in (report_a, report_b):
            self.assertTrue(all(event["entity_type"] for event in report["events"]))
            self.assertTrue(all(event["entity_id"] != "" for event in report["events"]))

    def test_failed_job_returns_to_queue_after_delay(self) -> None:
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
