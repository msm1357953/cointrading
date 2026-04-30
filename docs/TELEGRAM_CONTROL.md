# Telegram Control

## Design

Telegram is used in two layers:

1. Notifications: the bot sends status messages to one configured chat.
2. Commands: the VM polls Telegram for commands with `getUpdates`.

Long polling is preferred for now because it does not need an inbound HTTP server, DNS, TLS certificate, or extra firewall rule.

## Environment

The bot token can be set with either name:

```bash
TELEGRAM_BOT=...
TELEGRAM_BOT_TOKEN=...
```

Command handling needs an allowed chat ID:

```bash
TELEGRAM_CHAT_ID=123456789
TELEGRAM_ALLOWED_CHAT_IDS=123456789
TELEGRAM_COMMANDS_ENABLED=true
```

If `TELEGRAM_ALLOWED_CHAT_IDS` is empty, `TELEGRAM_CHAT_ID` is used as the only allowed chat.

## Discover Chat ID

Send a message such as `/start` to the bot, then run:

```bash
cd ~/cointrading
. .venv/bin/activate
python -m cointrading.cli telegram-updates --limit 5
```

Copy the printed `chat_id` into `.env`.

## Commands

- `도움말`: 사용 가능한 명령어와 chat ID를 보여줍니다.
- `상태`: 모드, dry-run, 정지 상태, 기준 자산을 보여줍니다.
- `계좌`: Binance 선물 계좌 요약을 보여줍니다.
- `위험`: 리스크 한도를 보여줍니다.
- `수수료`: BNB 수수료 할인 설정, USDC live 준비 상태, BTC/ETH USDT·USDC 현재 수수료를 보여줍니다.
- `가격 BTCUSDT`: 최근 가격을 보여줍니다.
- `스캘핑 BTCUSDT`: 실시간 dry-run 스캘핑 신호, 장 상태, 진입 허용 여부를 보여줍니다.
- `장상태 BTCUSDT`: `스캘핑 BTCUSDT`와 같습니다.
- `보고`: 전체 스캘핑 dry-run 채점 결과와 장 상태별 성과를 보여줍니다.
- `보고 BTCUSDT`: 특정 심볼의 스캘핑 dry-run 채점 결과와 장 상태별 성과를 보여줍니다.
- `정지`: 신규 진입 정지 상태로 둡니다.
- `재개`: 신규 진입 정지를 풉니다.

Slash commands such as `/status`, `/account`, `/scalp BTCUSDT`, and `/scalp_report` are still accepted.

No command places live orders. Future live-trading commands should require explicit two-step confirmation and should stay blocked while `COINTRADING_DRY_RUN=true`.

## Run Once

```bash
python -m cointrading.cli telegram-poll --once --timeout 10
```

## Run Continuously

```bash
python -m cointrading.cli telegram-poll --timeout 20 --interval 1
```

The continuous command is ready for a later `systemd` service, but it is not installed as a background service yet.
