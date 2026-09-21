import subprocess
import sys


CHECKS = [
    "monitoring/health_check.py",
    "monitoring/warehouse_health_check.py",
]


def main():
    failures = []

    for check in CHECKS:
        print("=" * 70)
        print(f"RUNNING: {check}")
        print("=" * 70)

        result = subprocess.run(
            [sys.executable, check],
            check=False,
        )

        if result.returncode != 0:
            failures.append(check)

        print()

    print("=" * 70)

    if failures:
        print("MONITORING SUITE FAILED")
        print("\nFailed checks:")

        for failure in failures:
            print(f"- {failure}")

        raise SystemExit(1)

    print("MONITORING SUITE PASSED")
    print("All monitoring checks completed successfully.")


if __name__ == "__main__":
    main()