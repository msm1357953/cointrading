# Deployment

## Current Shape

The repository stores code, tests, docs, and systemd units. Runtime secrets stay on the VM in `~/cointrading/.env`; they are not stored in GitHub.

The VM is the production-like runner:

- project path: `~/cointrading`
- static IP: `34.50.6.186`
- systemd units: `cointrading-telegram.service`, `cointrading-scalp-collect.timer`, `cointrading-scalp-score.timer`, `cointrading-market-regime.timer`, `cointrading-market-context.timer`, `cointrading-strategy-evaluate.timer`, `cointrading-strategy-notify.timer`, `cointrading-vibe-probe-notify.timer`, `cointrading-refine-entry-notify.timer`, `cointrading-live-supervisor-notify.timer`, `cointrading-trade-event-notify.timer`, `cointrading-llm-report.timer`, `cointrading-scalp-engine.timer`, `cointrading-strategy-engine.timer`

## GitHub Actions Deploy

The workflow `.github/workflows/deploy-vm.yml` is a manual deployment button. It SSHes to the VM, fetches `main`, preserves the VM `.env`, installs the package, runs tests, refreshes systemd units, and restarts the Telegram service.

Required GitHub repository secrets:

- `VM_HOST`
- `VM_USER`
- `VM_PROJECT_DIR`
- `VM_SSH_PRIVATE_KEY`

The deploy workflow intentionally uses `workflow_dispatch` instead of deploying every push. That keeps Codex Web useful for code changes while preserving a human deployment gate before touching the VM.

## Manual Deploy Path

From GitHub, open Actions, choose `Deploy to VM`, then run the workflow on `main`.

From a local machine with GCP access, deployment can still be done with `gcloud compute ssh` and `git pull` if GitHub Actions is unavailable.
