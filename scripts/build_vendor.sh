#!/usr/bin/env bash
set -euo pipefail

# Gera a pasta vendor/ com as dependências do projeto
# Uso: ./scripts/build_vendor.sh

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
VENDOR_DIR="$ROOT_DIR/vendor"

mkdir -p "$VENDOR_DIR"

# Pacotes definidos em pyproject.toml
python3 -m pip install --upgrade pip >/dev/null
python3 -m pip install -t "$VENDOR_DIR" \
  PyMySQL>=1.1 \
  python-dotenv>=1.0 \
  PyYAML>=6.0 \
  typer>=0.12 \
  rich>=13.7

echo "OK vendor pronto: $VENDOR_DIR"
echo "Para executar sem instalar: ./scripts/detraf run"

