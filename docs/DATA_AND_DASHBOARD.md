# Data and Dashboard

## SQLite Store

Runtime records live in `data/cointrading.sqlite`.

Tables:

- `signals`: dry-run scalp signals, market regime, fee estimates, and 1/3/5 minute forward scores.
- `orders`: post-only maker order attempts, blocked decisions, dry-run responses, and future live responses.
- `fills`: execution fills and realized fee/PnL records. This table is ready for exchange fill ingestion.
- `fee_snapshots`: maker/taker fee snapshots by symbol.

CSV files remain gitignored and are now treated as compatibility logs. Use `migrate-csv-to-db` to import old rows.

## Commands

```bash
python -m cointrading.cli migrate-csv-to-db
python -m cointrading.cli db-summary
python -m cointrading.cli scalp-report
python -m cointrading.cli maker-once --symbol BTCUSDC
python -m cointrading.cli dashboard --host 127.0.0.1 --port 8080
```

## Dashboard

The dashboard is a small local HTTP server. It shows the current scalp report, recent signals, and recent order or blocked-order records. It is intentionally bound to `127.0.0.1` by default; do not expose it publicly without authentication.

On the VM it runs as `cointrading-dashboard.service`, still bound to `127.0.0.1:8080`.
Use an SSH tunnel before opening it from a local browser:

```bash
gcloud compute ssh cointrading-vm --project=seokmin-494312 --zone=asia-northeast3-a -- -L 8080:127.0.0.1:8080
```
