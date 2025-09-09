"""Pacote principal do analisador DETRAF."""

# Evitamos importações pesadas aqui para reduzir efeitos colaterais durante
# ``import detraf``. Os submódulos podem ser acessados diretamente via
# ``import detraf.<modulo>`` quando necessário.

__all__ = ["__version__"]

__version__ = "2.0.0"
