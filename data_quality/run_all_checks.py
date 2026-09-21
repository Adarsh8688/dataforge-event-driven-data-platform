import subprocess
import sys


CHECKS = [
    "data_quality/warehouse_row_counts.py",
    "data_quality/warehouse_key_integrity.py",
    "data_quality/warehouse_referential_integrity.py",
    "data_quality/warehouse_measure_quality.py",
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
        print("DATA QUALITY SUITE FAILED")
        print("\nFailed checks:")

        for failure in failures:
            print(f"- {failure}")

        raise SystemExit(1)

    print("DATA QUALITY SUITE PASSED")
    print("All data quality checks completed successfully.")


if __name__ == "__main__":
    main()