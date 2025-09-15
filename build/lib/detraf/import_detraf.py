from pathlib import Path
from datetime import datetime

LAYOUT_DEFAULT = "configs/detraf_layout.yaml"

def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def _ok(msg: str) -> None:
    print(f"{_ts()} OK {msg}")

def _warn(msg: str) -> None:
    print(f"{_ts()} AVISO {msg}")

def _err(msg: str) -> None:
    print(f"{_ts()} ERRO {msg}")

def _count_lines(p: Path) -> int:
    # Contagem simples e eficiente
    total = 0
    with p.open("rb") as fh:
        for _ in fh:
            total += 1
    return total

def importar_arquivo_txt(caminho: str, periodo: str, eot: str, layout_path: str = LAYOUT_DEFAULT) -> dict:
    """
    Importa o arquivo DETRAF (layout fixo) para a tabela detraf via rotina fixowidth.
    Retorna um resumo no formato:
      {
        "total": int,
        "inseridos": int,
        "ignorados_inconsistentes": int,
        "duracao": float (opcional),
        ...
      }
    """
    p = Path(caminho)
    if not p.exists() or not p.is_file():
        raise FileNotFoundError(f"Arquivo não encontrado: {caminho}")

    ly = Path(layout_path)
    if not ly.exists() or not ly.is_file():
        raise FileNotFoundError(f"Layout não encontrado: {layout_path}")

    total = _count_lines(p)
    _ok(f"Arquivo encontrado: {p.name} | {total} linhas detectadas")

    # Importer oficial (fixowidth)
    from .import_detraf_fw import importar_fixowidth_para_detraf

    # Tenta com (caminho, layout, periodo, eot); se não for suportado, faz fallback (caminho, layout)
    resumo: dict
    try:
        resumo = importar_fixowidth_para_detraf(str(p), str(ly), periodo, eot)  # assinatura nova
    except TypeError:
        _warn("Rotina fixowidth não aceita periodo/eot; usando assinatura antiga (caminho, layout).")
        resumo = importar_fixowidth_para_detraf(str(p), str(ly))  # assinatura antiga

    # Normaliza chaves do resumo
    if "total" not in resumo:
        resumo["total"] = total
    # compatibilidade com possíveis nomes
    inseridos = (
        resumo.get("inseridos")
        or resumo.get("inserted")
        or resumo.get("gravados")
        or 0
    )
    resumo["inseridos"] = int(inseridos)

    ignorados = (
        resumo.get("ignorados_inconsistentes")
        or resumo.get("ignored_invalid")
        or resumo.get("ignorados")
        or 0
    )
    resumo["ignorados_inconsistentes"] = int(ignorados)

    _ok(f"Arquivo importado e persistido ({resumo['inseridos']} linhas).")
    return resumo
