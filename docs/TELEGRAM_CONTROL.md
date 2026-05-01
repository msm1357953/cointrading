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
- `수수료`: BNB 수수료 할인 설정, USDC live 준비 상태, BTC/ETH USDC 현재 수수료를 보여줍니다.
- `가격 BTCUSDC`: 최근 가격을 보여줍니다.
- `장세`: 큰 장상태와 현재 허용 전략 세트를 보여줍니다.
- `장세 BTCUSDC`: 특정 심볼의 큰 장상태와 허용 전략 세트를 보여줍니다.
- `시장상황`: 펀딩, 프리미엄, 미결제약정, 호가 유동성을 새로 수집해 보여줍니다.
- `스캘핑 BTCUSDC`: 실시간 dry-run 스캘핑 신호, 장 상태, 진입 허용 여부를 보여줍니다.
- `장상태 BTCUSDC`: `장세 BTCUSDC`와 같습니다.
- `진입 ETHUSDC 25`: 해당 심볼/규모에 대해 스캘핑, RSI/EMA 추세, RSI/볼린저 평균회귀, 돌파 후보를 분리해서 점검합니다. 이 명령은 주문을 넣지 않습니다.
- `실전 ETHUSDC 25`: 시장상황과 장세를 새로 수집한 뒤 실전 가능/불가 최종 감독 판정을 보여줍니다. 이 명령도 주문을 넣지 않습니다.
- 자동 후보 알림: VM은 `live-supervisor-notify`를 주기적으로 실행해 안전잠금만 남은 진입 후보가 새로 생기면 텔레그램으로 알려줍니다. 이 알림도 주문을 넣지 않고, `실전 80` 재확인과 수동 승인 전까지 live 플래그는 꺼진 상태를 유지합니다.
- `보고`: 전체 스캘핑 dry-run 채점 결과와 장 상태별 성과를 보여줍니다.
- `보고 BTCUSDC`: 특정 심볼의 스캘핑 dry-run 채점 결과와 장 상태별 성과를 보여줍니다.
- `보고 전체`: 예전 USDT 로그까지 포함한 전체 결과를 보여줍니다.
- `전략`: 신호 로그와 paper 상태머신 기반 전략 후보평가, live 잠금 상태, 진행 중인 전략 상태머신을 요약합니다.
- `주문`: 최근 dry-run 주문 또는 차단 기록을 보여줍니다.
- `포지션`: 최근 스캘핑/전략 상태머신 기록과 paper/live 손익을 보여줍니다.
- `정지`: 신규 진입 정지 상태로 둡니다.
- `재개`: 신규 진입 정지를 풉니다.

Slash commands such as `/status`, `/account`, `/scalp BTCUSDC`, and `/scalp_report` are still accepted.

Strategy notification messages are candidate evaluations, not trade confirmations. They group duplicated TP/SL/hold-time parameter variants so `APPROVED` counts do not look like separate live orders.

No Telegram command places live orders. Future live-trading commands should require explicit two-step confirmation and should stay blocked while `COINTRADING_DRY_RUN=true`. Macro strategy live orders also require `COINTRADING_LIVE_STRATEGY_LIFECYCLE_ENABLED=true`. Live entries additionally require the one-shot guard unless `COINTRADING_LIVE_ONE_SHOT_REQUIRED=false`.

## Run Once

```bash
python -m cointrading.cli telegram-poll --once --timeout 10
```

## Run Continuously

```bash
python -m cointrading.cli telegram-poll --timeout 20 --interval 1
```

The continuous command is ready for a later `systemd` service, but it is not installed as a background service yet.
