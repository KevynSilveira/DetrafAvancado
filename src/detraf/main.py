from __future__ import annotations
"""Entrada programática para executar o pipeline DETRAF."""

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
    # Importações locais para evitar dependências pesadas durante o import do
    # módulo ``detraf``. Cada passo do pipeline é encapsulado em seu próprio
    # módulo e chamado aqui sequencialmente.
    from .processing import begin_processing
    from .import_detraf import importar_arquivo_txt
    from .match_cdr import processar_match

    begin_processing(periodo, eot, arquivo)
    importar_arquivo_txt(arquivo, periodo, eot)
    processar_match()
