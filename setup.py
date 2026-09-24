"""本地安装入口。"""

from setuptools import find_packages, setup


setup(
    name="robot-trials-foundation",
    version="0.1.0",
    description="人形机器人试验统计准入服务",
    long_description=open("README.md", encoding="utf-8").read(),
    long_description_content_type="text/markdown",
    package_dir={"": "src"},
    packages=find_packages("src"),
    python_requires=">=3.11",
)
