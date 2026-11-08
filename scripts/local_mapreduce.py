"""Exécute les jobs MapReduce de mapreduce/src en local, sans Hadoop.

Reproduit Hadoop Streaming : mapper -> tri par clé (shuffle & sort) ->
combiner -> tri -> reducer, avec les scripts inchangés de l'Étudiant 1.
Puis écrit les TSV, les convertit en JSON via scripts/export_to_json.py et
produit un rapport qualité minimal. Sert aux tests d'intégration et à la
démonstration sur l'échantillon synthétique.

Usage : python scripts/local_mapreduce.py --transactions data/sample/transactions_sample.csv \\
                                          --root build/sample
        -> build/sample/results/*.tsv, *.json, data_quality_report.json
"""

import argparse
import csv
import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "mapreduce" / "src"
JOBS = ("product_count", "product_revenue", "customer_total", "average_price", "top10")


def _run(script: str, job: str, lines: list[str]) -> list[str]:
    result = subprocess.run(
        [sys.executable, str(SRC / script)],
        input="".join(lines),
        capture_output=True,
        text=True,
        encoding="utf-8",
        env={**os.environ, "JOB": job},
        check=True,
    )
    return result.stdout.splitlines(keepends=True)


def _shuffle(lines: list[str]) -> list[str]:
    # Hadoop trie sur la clé (texte avant la première tabulation).
    return sorted(lines, key=lambda line: line.split("\t", 1)[0])


def run_job(job: str, csv_text: str, use_combiner: bool = True) -> list[str]:
    mapped = _shuffle(_run("mapper.py", job, [csv_text]))
    if use_combiner and job != "top10":
        mapped = _shuffle(_run("combiner.py", job, mapped))
    return _run("reducer.py", job, mapped)


def quality_report(csv_path: Path) -> dict:
    rows, revenue, products, customers = 0, 0.0, set(), set()
    with csv_path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            rows += 1
            revenue += float(row["total_amount"])
            products.add(row["product_id"])
            if row["customer_id"]:
                customers.add(row["customer_id"])
    return {
        "source": f"Échantillon synthétique ({csv_path.name})",
        "initial_rows": rows,
        "final_rows": rows,
        "removed_rows": 0,
        "total_revenue": round(revenue, 2),
        "unique_products": len(products),
        "unique_customers": len(customers),
    }


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--transactions", type=Path, required=True)
    parser.add_argument("--root", type=Path, required=True, help="dossier de sortie (results/ y est créé)")
    args = parser.parse_args(argv)

    results = args.root / "results"
    results.mkdir(parents=True, exist_ok=True)
    csv_text = args.transactions.read_text(encoding="utf-8")
    for job in JOBS:
        output = run_job(job, csv_text)
        (results / f"{job}.tsv").write_text("".join(output), encoding="utf-8")
        print(f"{job:<16} {len(output):>6} lignes")

    subprocess.run([sys.executable, str(ROOT / "scripts" / "export_to_json.py")], cwd=args.root, check=True)
    report = quality_report(args.transactions)
    (results / "data_quality_report.json").write_text(json.dumps(report, indent=4, ensure_ascii=False), encoding="utf-8")
    print(f"Résultats dans {results}")


if __name__ == "__main__":
    main()
