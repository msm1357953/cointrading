# GCP VM Setup

## Current VM

- Project: `seokmin-494312`
- Instance: `cointrading-vm`
- Zone: `asia-northeast3-a`
- Region: `asia-northeast3`
- Machine type: `e2-small` (2 vCPU, 2 GB memory)
- OS: Ubuntu 24.04 LTS
- Boot disk: 20 GB balanced persistent disk
- Static external IP: `34.50.6.186`
- Internal IP: `10.178.0.2`

## Binance API Whitelist

Use this VM external IP for Binance API trusted IP access:

```text
34.50.6.186
```

The VM outbound IP was verified with `curl https://api.ipify.org`.

## Local Commands

SSH:

```bash
gcloud compute ssh cointrading-vm \
  --project=seokmin-494312 \
  --zone=asia-northeast3-a
```

Run the current dry-run project:

```bash
cd ~/cointrading
. .venv/bin/activate
python -m cointrading.cli demo-backtest
python -m cointrading.cli fetch-klines --symbol BTCUSDT --interval 1h --limit 10
```

Telegram command service:

```bash
sudo systemctl status cointrading-telegram --no-pager
sudo journalctl -u cointrading-telegram -n 50 --no-pager
systemctl list-timers 'cointrading-*'
```

## Notes

- The VM uses the default GCP SSH firewall behavior.
- Project files are copied to `~/cointrading` on the VM.
- The `.env` file exists on the VM and is permissioned as owner read/write only.
- `COINTRADING_DRY_RUN=true` remains enabled.
- Do not disable dry-run until Binance Futures API permission and testnet/paper behavior are verified.
- Telegram polling is managed by `cointrading-telegram.service` on the VM.
- Dry-run scalping signal collection is managed by `cointrading-scalp-collect.timer` and `cointrading-scalp-score.timer`.
- Macro strategy lifecycle dry-run/paper cycles are managed by `cointrading-strategy-engine.timer`.
