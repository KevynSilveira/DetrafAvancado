from __future__ import annotations
"""Entrada programática para executar o pipeline DETRAF."""

from .import_detraf import importar_arquivo_txt
from .match_cdr import processar_match
from .processing import begin_processing


def executar(periodo: str, eot: str, arquivo: str) -> None:
    """Executa a preparação, importação e o batimento completo.

    Parâmetros
    ----------
    periodo: str
        Mês de referência no formato ``YYYYMM``.
    eot: str
        Código EOT a ser aplicado durante a importação.
    arquivo: str
        Caminho para o arquivo DETRAF fornecido pela operadora.
    """
    begin_processing(periodo, eot, arquivo)
    importar_arquivo_txt(arquivo, periodo, eot)
    processar_match(periodo)
