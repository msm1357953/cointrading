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

The dashboard is a small HTTP server. It shows the current scalp report, recent signals, and recent order or blocked-order records.

Set `COINTRADING_DASHBOARD_AUTH_TOKEN` before exposing it outside the VM.
When the token is set, requests must include either `?token=...` or an `Authorization: Bearer ...` header.

On the VM it runs as `cointrading-dashboard.service`, bound to `0.0.0.0:8080` for mobile/browser access.

```bash
curl -H "Authorization: Bearer $COINTRADING_DASHBOARD_AUTH_TOKEN" http://34.50.6.186:8080/
```
