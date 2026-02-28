# 🛡️ Inventory Integrity & Lineage Pipeline

An automated **Data Quality & Governance System** designed to reconcile complex inbound/outbound logistics and protect against inventory-based revenue leakage.

This project demonstrates **Production-Grade Data Engineering** by combining automated schema validation with deep-dive relational lineage.

## 🚀 Key Engineering Features

* **Automated CI/CD Auditing:** Integrated with **GitHub Actions** to run deep-dive logic checks at 00:16 UTC daily.
* **Immutable Data Lineage:** Every audit record is stamped with `_source_table`, `_audit_run_at`, and a `_lineage_key` (Hash) to ensure 100% traceability from source to report.
* **Stateful Ledger Logic:** Uses **Advanced SQL Window Functions** to calculate daily running totals, identifying "Oversold" events and "Orphaned Sales" (Product IDs with no stock history).
* **Failure Artifacts:** Automated "Snapshots" of data failures are generated and preserved as GitHub Artifacts whenever a business rule is violated.

## 🛠️ Tech Stack

* **SQL Engine:** DuckDB (OLAP)
* **Validation:** Pandera (Lazy-evaluation Schema enforcement)
* **Automation:** GitHub Actions (CI/CD)
* **Logic:** Advanced SQL (CTEs, Window Functions, Metadata Tagging)
* **Language:** Python 3.12



## 📊 Pipeline Logic

1. **Extraction:** GitHub Actions wakes up and extracts data from the `warehouse.db`.
2. **Schema Audit:** Pandera validates data types and business constraints (e.g., `qty < 200`).
3. **Logic Audit:** A Vertical Union combines Stock/Sales into a Ledger.
4. **Lineage Stamping:** Metadata columns are injected to track the "Family Tree" of the row.
5. **Reporting:** If running totals dip below zero, the system triggers a failure and exports a `data_health_issues.csv`.

## ⚙️ Installation

1. **Clone & Setup:**
   ```bash
   git clone [https://github.com/yourusername/inventory-lineage-audit.git](https://github.com/yourusername/inventory-lineage-audit.git)
   cd inventory-lineage-audit
   python -m venv .venv
   source .venv/bin/activate  # or .venv\Scripts\activate
   pip install -r requirements.txt