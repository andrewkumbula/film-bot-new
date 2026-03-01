#!/usr/bin/env bash
# Регрессионные тесты перед коммитом/деплоем. Запуск: ./scripts/run_tests.sh или pytest
set -e
cd "$(dirname "$0")/.."
export BOT_TOKEN="${BOT_TOKEN:-test-token}"
export OPENROUTER_API_KEY="${OPENROUTER_API_KEY:-test-key}"
python -m pytest tests/ -v "$@"
