from datetime import datetime
import calendar

def yyyymm_to_window_3m(yyyymm: str) -> tuple[datetime, datetime]:
    """Converte YYYYMM em janela de 3 meses:
    início = 1º dia de (M-2) 00:00:00
    fim    = último dia de M 23:59:59
    Ex.: 202505 -> 202503-01 00:00:00 até 2025-05-31 23:59:59
    """
    ano = int(yyyymm[:4])
    mes = int(yyyymm[4:6])

    mes_ini = mes - 2
    ano_ini = ano
    while mes_ini <= 0:
        mes_ini += 12
        ano_ini -= 1

    ini = datetime(ano_ini, mes_ini, 1, 0, 0, 0)
    last_day = calendar.monthrange(ano, mes)[1]
    fim = datetime(ano, mes, last_day, 23, 59, 59)
    return ini, fim
