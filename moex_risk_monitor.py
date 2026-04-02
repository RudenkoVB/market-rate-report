#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Отчёт MOEX: цены и ставки риска 1-го уровня (публичный ISS API).

Запуск: python moex_risk_monitor.py
Создаются moex_report.html и moex_report_data.json в этой папке; открывается HTML в браузере.

Ставки риска 1 ур.: публичные таблицы RMS MOEX ISS (im1 для фондового и валютного рынков,
mr1 для срочного) — те же значения, что публикуются в статических риск-параметрах НКЦ
(https://www.nationalclearingcentre.ru/), в машиночитаемом виде доступны через ISS.
Кэш истории: папка moex_cache/; объёмы валют по датам — currency_volume_by_date.json.
"""

from __future__ import annotations

import json
import math
import os
import re
import urllib.parse
import urllib.request
import webbrowser
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, Iterable, List, Optional, Tuple

ISS = "https://iss.moex.com/iss"
PAGESIZE = 100
EOD_LOOKBACK_DAYS = 45
# Сколько торговых дней в блоке «Итоги торгового дня» (1 = только последний завершённый день с ценами закрытия).
EOD_SESSIONS_IN_REPORT = 1

# Фьючерс: только краткое имя вида Si-6.26 (без спредов и опционов)
FUT_NAME_RE = re.compile(r"^([A-Za-z][A-Za-z0-9]*)-(\d{1,2})\.(\d{2})$")

SCRIPT_DIR = Path(__file__).resolve().parent
CACHE_DIR = SCRIPT_DIR / "moex_cache"
VOLUME_CACHE_FILE = CACHE_DIR / "currency_volume_by_date.json"
CACHE_VER = "v2"

MOEX_CONTRACT_PAGE = "https://www.moex.com/ru/contract.aspx"
FUT_MOEX_TITLES_FILE = CACHE_DIR / f"fut_moex_titles_{CACHE_VER}.json"
_MOEX_TITLE_CACHE: Optional[Dict[str, str]] = None


def fetch_text(url: str, timeout: float = 30.0) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": "moex-risk-monitor/3.0 (report)"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="replace")


def moex_contract_page_url(shortname: str) -> str:
    code = (shortname or "").strip().lower()
    return f"{MOEX_CONTRACT_PAGE}?{urllib.parse.urlencode({'code': code})}"


def parse_moex_contract_title(html: str) -> Optional[str]:
    """Текст после «КОД:» в h1 или из <title> (как на страницах contract.aspx)."""
    m = re.search(r"<h1[^>]*>(.*?)</h1>", html, re.I | re.DOTALL)
    if m:
        inner = re.sub(r"<[^>]+>", "", m.group(1))
        inner = re.sub(r"\s+", " ", inner).strip()
        if ":" in inner:
            rest = inner.split(":", 1)[1].strip()
            if rest and len(rest) > 2:
                return rest
    m = re.search(r"<title>\s*([^<]+)</title>", html, re.I)
    if m:
        t = m.group(1).split("|")[0].strip()
        t = re.sub(r"\s*\([^)]*\)\s*котировки.*$", "", t, flags=re.I).strip()
        if "контракт" in t.lower() or "фьючерс" in t.lower():
            return t
    return None


def load_moex_title_cache() -> Dict[str, str]:
    global _MOEX_TITLE_CACHE
    if _MOEX_TITLE_CACHE is not None:
        return _MOEX_TITLE_CACHE
    if FUT_MOEX_TITLES_FILE.is_file():
        with open(FUT_MOEX_TITLES_FILE, "r", encoding="utf-8") as f:
            _MOEX_TITLE_CACHE = json.load(f)
    else:
        _MOEX_TITLE_CACHE = {}
    return _MOEX_TITLE_CACHE


def prefetch_moex_futures_titles(shortnames: Iterable[str]) -> None:
    """Подписи с сайта MOEX, напр. https://www.moex.com/ru/contract.aspx?code=orange-5.26"""
    global _MOEX_TITLE_CACHE
    cache = load_moex_title_cache()
    need: List[str] = []
    seen: set = set()
    for sn in shortnames:
        s = (sn or "").strip()
        if not s or s in seen:
            continue
        seen.add(s)
        if s not in cache:
            need.append(s)
    if not need:
        return

    def fetch_one(s: str) -> Tuple[str, str]:
        try:
            html = fetch_text(moex_contract_page_url(s), timeout=28.0)
            t = parse_moex_contract_title(html)
            return s, (t.strip() if t else "")
        except Exception:
            return s, ""

    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = [ex.submit(fetch_one, s) for s in need]
        for fut in as_completed(futs):
            s, val = fut.result()
            cache[s] = val

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with open(FUT_MOEX_TITLES_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False)
    _MOEX_TITLE_CACHE = cache


def moex_futures_display_name(shortname: str) -> str:
    """Пустая строка, если название не найдено."""
    s = (shortname or "").strip()
    if not s:
        return ""
    return (load_moex_title_cache().get(s) or "").strip()


@dataclass
class MarketConfig:
    key: str
    title: str
    engine: str
    market: str
    history_boards: Tuple[str, ...]
    live_boards: Tuple[str, ...]
    risk_kind: str


MARKETS: Dict[str, MarketConfig] = {
    "stock": MarketConfig(
        key="stock",
        title="Фондовый рынок",
        engine="stock",
        market="shares",
        history_boards=("TQBR",),
        live_boards=("TQBR",),
        risk_kind="im",
    ),
    "currency": MarketConfig(
        key="currency",
        title="Валютный рынок",
        engine="currency",
        market="selt",
        history_boards=("CETS",),
        live_boards=("CETS",),
        risk_kind="im",
    ),
    "futures": MarketConfig(
        key="futures",
        title="Срочный рынок",
        engine="futures",
        market="forts",
        history_boards=("RFUD",),
        live_boards=("RFUD",),
        risk_kind="mr",
    ),
}


def fetch_json(url: str, timeout: float = 90.0) -> Any:
    req = urllib.request.Request(url, headers={"User-Agent": "moex-risk-monitor/2.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def iss_table_rows(data: Any, name: str) -> Tuple[List[str], List[List[Any]]]:
    block = data.get(name) or {}
    return block.get("columns") or [], block.get("data") or []


def paginate_iss(path: str, table: str, extra: Optional[Dict[str, str]] = None) -> List[List[Any]]:
    out: List[List[Any]] = []
    start = 0
    while True:
        q: Dict[str, str] = {"iss.meta": "off", "start": str(start)}
        if extra:
            q.update(extra)
        url = f"{ISS}{path}?{urllib.parse.urlencode(q)}"
        data = fetch_json(url)
        _, rows = iss_table_rows(data, table)
        if not rows:
            break
        out.extend(rows)
        if len(rows) < PAGESIZE:
            break
        start += PAGESIZE
    return out


def load_risk_limits_stock() -> Dict[str, float]:
    """Ставка риска 1 ур. (im1) по инструменту (SECID)."""
    rows = paginate_iss("/rms/engines/stock/objects/limits.json", "limits")
    m: Dict[str, float] = {}
    for r in rows:
        if len(r) < 5:
            continue
        try:
            m[str(r[1])] = float(r[3]) / 100.0
        except (TypeError, ValueError):
            continue
    return m


def load_risk_limits_currency() -> Dict[str, float]:
    """Ставка риска 1 ур. (im1) для сделок XXX/RUB: ключ — код валюты XXX (не перетираем кросс EUR/USD)."""
    rows = paginate_iss("/rms/engines/currency/objects/limits.json", "limits")
    m: Dict[str, float] = {}
    for r in rows:
        if len(r) < 5:
            continue
        if str(r[2]).upper() != "RUB":
            continue
        try:
            m[str(r[1])] = float(r[3]) / 100.0
        except (TypeError, ValueError):
            continue
    return m


def load_risk_limits_futures() -> Dict[str, float]:
    """Ставка риска 1 ур. (mr1) по коду базового актива (ASSETCODE)."""
    rows = paginate_iss("/rms/engines/futures/objects/limits.json", "limits")
    m: Dict[str, float] = {}
    for r in rows:
        if len(r) < 5:
            continue
        try:
            m[str(r[1])] = float(r[2])
        except (TypeError, ValueError):
            continue
    return m


def parse_futures_expiry_mmyy(shortname: str) -> Optional[Tuple[int, int]]:
    """Краткое имя вида ALUM-6.26 → (год 2026, месяц 6) для сортировки сроков."""
    m = FUT_NAME_RE.match((shortname or "").strip())
    if not m:
        return None
    mon = int(m.group(2))
    yy = int(m.group(3))
    year = 2000 + yy if yy < 80 else 1900 + yy
    return year, mon


def annotate_futures_maturity_rank(rows: List[Dict[str, Any]]) -> None:
    """Внутри каждого базового актива (ASSETCODE) ранжируем контракты по сроку (1 — ближайший)."""
    groups: Dict[str, List[Tuple[Tuple[int, int], Dict[str, Any]]]] = defaultdict(list)
    for r in rows:
        ac = (r.get("asset_code") or "").strip()
        if not ac:
            tm = FUT_NAME_RE.match((r.get("ticker") or "").strip())
            if tm:
                ac = tm.group(1)
        if not ac:
            r["fut_rank"] = 999
            continue
        sn = (r.get("ticker") or r.get("shortname") or "").strip()
        exp = parse_futures_expiry_mmyy(sn)
        if exp is None:
            r["fut_rank"] = 999
            continue
        groups[ac].append((exp, r))
    for _ac, items in groups.items():
        items.sort(key=lambda x: x[0])
        for rank, (_exp, row) in enumerate(items, start=1):
            row["fut_rank"] = rank


_FUT_SECMETA_CACHE: Optional[Dict[str, Dict[str, Any]]] = None
FUT_SECMETA_FILE = CACHE_DIR / f"fut_secmeta_{CACHE_VER}.json"


def load_futures_secmeta_for(secids: Iterable[str]) -> Dict[str, Dict[str, Any]]:
    """SHORTNAME/SECNAME/ASSETCODE только для переданных SECID (пакеты ISS, кэш на диске)."""
    global _FUT_SECMETA_CACHE
    if _FUT_SECMETA_CACHE is None:
        if FUT_SECMETA_FILE.is_file():
            with open(FUT_SECMETA_FILE, "r", encoding="utf-8") as f:
                _FUT_SECMETA_CACHE = json.load(f)
        else:
            _FUT_SECMETA_CACHE = {}
    store = _FUT_SECMETA_CACHE
    want = {str(s) for s in secids}
    missing = [s for s in want if s not in store]
    batch_size = 40
    for i in range(0, len(missing), batch_size):
        chunk = missing[i : i + batch_size]
        q = urllib.parse.urlencode(
            {
                "iss.meta": "off",
                "securities": ",".join(chunk),
                "securities.columns": "SECID,SHORTNAME,SECNAME,ASSETCODE",
            }
        )
        data = fetch_json(f"{ISS}/engines/futures/markets/forts/securities.json?{q}")
        _, rows = iss_table_rows(data, "securities")
        scols = data.get("securities", {}).get("columns") or []
        if not scols:
            continue
        si = {c: i for i, c in enumerate(scols)}
        for row in rows:
            sid = str(row[si["SECID"]])
            store[sid] = {
                "SHORTNAME": row[si["SHORTNAME"]],
                "SECNAME": row[si["SECNAME"]] if "SECNAME" in si else "",
                "ASSETCODE": row[si["ASSETCODE"]] if "ASSETCODE" in si else None,
            }
    if missing:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        with open(FUT_SECMETA_FILE, "w", encoding="utf-8") as f:
            json.dump(store, f, ensure_ascii=False)
    return {s: store.get(s, {}) for s in want}


def currency_asset_from_secid(secid: str, known_assets: List[str]) -> Optional[str]:
    u = secid.upper()
    for a in sorted(known_assets, key=len, reverse=True):
        if len(a) >= 3 and a in u:
            return a
    m = re.match(r"^([A-Z]{3})RUB", u)
    if m and m.group(1) in known_assets:
        return m.group(1)
    return None


def is_currency_tom(secid: str) -> bool:
    """Основные TOM-пары: USDRUB_TOM, CNYRUB_TOM и т.п."""
    return secid.upper().endswith("_TOM")


def is_futures_contract(shortname: str, assetcode: Optional[str]) -> bool:
    """Только фьючерсы с кратким именем вида Si-6.26 (не спреды, не пустой базовый актив)."""
    sn = (shortname or "").strip()
    if not FUT_NAME_RE.match(sn):
        return False
    if assetcode is None or not str(assetcode).strip():
        return False
    return True


def risk_is_hundred_percent(rk: Optional[float]) -> bool:
    return rk is not None and rk >= 1.0 - 1e-9


def cache_file_history(cfg: MarketConfig, tradedate: str) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"hist_{CACHE_VER}_{cfg.engine}_{cfg.market}_{tradedate}.json"


def load_history_cached(cfg: MarketConfig, tradedate: str) -> Dict[str, Dict[str, Any]]:
    path = cache_file_history(cfg, tradedate)
    if path.is_file():
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    path_part = f"/history/engines/{cfg.engine}/markets/{cfg.market}/securities.json"
    by_sec: Dict[str, Dict[str, Any]] = {}
    start = 0
    while True:
        q = urllib.parse.urlencode(
            {"date": tradedate, "iss.meta": "off", "start": str(start), "limit": str(PAGESIZE)}
        )
        data = fetch_json(f"{ISS}{path_part}?{q}")
        hcols, rows = iss_table_rows(data, "history")
        if not hcols or not rows:
            break
        idx = {c: i for i, c in enumerate(hcols)}
        for row in rows:
            board = row[idx["BOARDID"]]
            if board not in cfg.history_boards:
                continue
            secid = row[idx["SECID"]]
            rec = {hcols[i]: row[i] for i in range(len(hcols))}
            if cfg.key == "currency" and not is_currency_tom(secid):
                continue
            if cfg.key == "futures":
                sn = rec.get("SHORTNAME") or ""
                ac = rec.get("ASSETCODE")
                if not is_futures_contract(str(sn), str(ac) if ac is not None else None):
                    continue
            by_sec[secid] = rec
        start += PAGESIZE
        if len(rows) < PAGESIZE:
            break
    with open(path, "w", encoding="utf-8") as f:
        json.dump(by_sec, f, ensure_ascii=False)
    return by_sec


def close_from_history_row(rec: Dict[str, Any], engine: str) -> Optional[float]:
    if engine == "futures":
        for key in ("SETTLEPRICE", "WAPRICE", "CLOSE"):
            v = rec.get(key)
            if v is not None:
                try:
                    fv = float(v)
                    if fv > 0:
                        return fv
                except (TypeError, ValueError):
                    pass
        return None
    for key in ("LEGALCLOSEPRICE", "CLOSE", "WAPRICE"):
        v = rec.get(key)
        if v is not None:
            try:
                fv = float(v)
                if fv > 0:
                    return fv
            except (TypeError, ValueError):
                pass
    return None


def volume_rub_from_history(rec: Dict[str, Any], engine: str) -> Optional[float]:
    if engine == "futures":
        v = rec.get("VALUE")
        if v is not None:
            try:
                return float(v)
            except (TypeError, ValueError):
                pass
        return None
    if engine == "stock":
        v = rec.get("VALUE")
        if v is not None:
            try:
                return float(v)
            except (TypeError, ValueError):
                pass
    return None


def pick_futures_sample_sec() -> str:
    start = 0
    while start < 3000:
        data = fetch_json(
            f"{ISS}/engines/futures/markets/forts/securities.json?"
            + urllib.parse.urlencode(
                {"iss.meta": "off", "securities.columns": "SECID,SHORTNAME", "start": str(start), "limit": "100"}
            )
        )
        _, rows = iss_table_rows(data, "securities")
        if not rows:
            break
        for r in rows:
            if len(r) >= 2 and FUT_NAME_RE.match(str(r[1] or "")):
                return str(r[0])
        start += 100
        if len(rows) < 100:
            break
    return "SiM6"


def get_three_trading_days(cfg: MarketConfig, report_end: date) -> Tuple[str, str, str]:
    if cfg.key == "futures":
        sample = pick_futures_sample_sec()
    elif cfg.key == "currency":
        sample = "CNYRUB_TOM"
    else:
        sample = "SBER"
    frm = (report_end - timedelta(days=EOD_LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    till = report_end.strftime("%Y-%m-%d")
    path = f"/history/engines/{cfg.engine}/markets/{cfg.market}/securities/{sample}.json"
    url = f"{ISS}{path}?{urllib.parse.urlencode({'from': frm, 'till': till, 'iss.meta': 'off'})}"
    data = fetch_json(url)
    _, rows = iss_table_rows(data, "history")
    cols = data.get("history", {}).get("columns", [])
    if not cols or not rows:
        raise RuntimeError("Нет данных календаря торгов.")
    idx = {c: i for i, c in enumerate(cols)}
    till_s = till
    dates = sorted({row[idx["TRADEDATE"]] for row in rows if row[idx["TRADEDATE"]] <= till_s})
    if len(dates) < 3:
        raise RuntimeError("Недостаточно торговых дней.")
    return dates[-1], dates[-2], dates[-3]


def risk_for_security(
    cfg: MarketConfig,
    secid: str,
    assetcode: Optional[str],
    stock_lim: Dict[str, float],
    cur_lim: Dict[str, float],
    fut_lim: Dict[str, float],
    cur_assets: List[str],
) -> Optional[float]:
    if cfg.risk_kind == "im" and cfg.key == "stock":
        return stock_lim.get(secid)
    if cfg.risk_kind == "im" and cfg.key == "currency":
        a = currency_asset_from_secid(secid, cur_assets)
        return cur_lim.get(a) if a else None
    if cfg.risk_kind == "mr" and assetcode:
        return fut_lim.get(str(assetcode).strip())
    return None


def row_highlight(r1: Optional[float], r2: Optional[float]) -> str:
    vals = [x for x in (r1, r2) if x is not None and not math.isnan(x)]
    if not vals:
        return ""
    m = max(vals)
    if m >= 100:
        return "hl-crimson"
    if m >= 80:
        return "hl-red"
    if m >= 50:
        return "hl-yellow"
    return ""


def ratio_pct(change_pct: Optional[float], risk: Optional[float]) -> Optional[float]:
    if change_pct is None or risk is None or risk <= 0:
        return None
    return abs(change_pct) / (risk * 100.0) * 100.0


def load_volume_cache() -> Dict[str, Dict[str, float]]:
    if not VOLUME_CACHE_FILE.is_file():
        return {}
    with open(VOLUME_CACHE_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def save_volume_cache(vol: Dict[str, Dict[str, float]]) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with open(VOLUME_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(vol, f, ensure_ascii=False)


def fetch_currency_session_volumes() -> Tuple[str, Dict[str, float]]:
    """VALTODAY_RUR по инструментам CETS TOM и дата торгов из dataversion."""
    path = "/engines/currency/markets/selt/securities.json"
    q = urllib.parse.urlencode(
        {
            "iss.meta": "off",
            "securities.columns": "SECID,BOARDID",
            "marketdata.columns": "SECID,BOARDID,VALTODAY_RUR",
        }
    )
    data = fetch_json(f"{ISS}{path}?{q}")
    _, md_rows = iss_table_rows(data, "marketdata")
    scols = data["securities"]["columns"]
    mcols = data.get("marketdata", {}).get("columns") or []
    si = {c: i for i, c in enumerate(scols)}
    mi = {c: i for i, c in enumerate(mcols)}
    sess_date = ""
    dv = data.get("dataversion", {}).get("data") or []
    if dv and len(dv[0]) > 2:
        sess_date = str(dv[0][2] or "")
    out: Dict[str, float] = {}
    for row in md_rows:
        if row[mi["BOARDID"]] != "CETS":
            continue
        sid = row[mi["SECID"]]
        if not is_currency_tom(sid):
            continue
        if "VALTODAY_RUR" not in mi:
            continue
        vr = row[mi["VALTODAY_RUR"]]
        if vr is not None:
            try:
                out[sid] = float(vr)
            except (TypeError, ValueError):
                pass
    return sess_date, out


def merge_currency_volumes(vol_cache: Dict[str, Dict[str, float]]) -> Dict[str, Dict[str, float]]:
    sess, m = fetch_currency_session_volumes()
    if sess and m:
        vol_cache[sess] = m
        save_volume_cache(vol_cache)
    return vol_cache


def live_market_block(cfg: MarketConfig) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
    if cfg.key == "currency":
        sec_cols = "SECID,SHORTNAME,SECNAME,BOARDID"
    elif cfg.key == "futures":
        sec_cols = "SECID,SHORTNAME,SECNAME,BOARDID,ASSETCODE"
    else:
        sec_cols = "SECID,SHORTNAME,SECNAME,BOARDID"
    q = urllib.parse.urlencode(
        {
            "iss.meta": "off",
            "securities.columns": sec_cols,
            "marketdata.columns": "SECID,BOARDID,LAST,MARKETPRICE,LCURRENTPRICE,WAPRICE,VALTODAY_RUR,VALTODAY,VALUE,NUMTRADES",
        }
    )
    data = fetch_json(f"{ISS}/engines/{cfg.engine}/markets/{cfg.market}/securities.json?{q}")
    mcols = data.get("marketdata", {}).get("columns") or []
    _, md_rows = iss_table_rows(data, "marketdata")
    mi = {c: i for i, c in enumerate(mcols)}
    scols = data["securities"]["columns"]
    _, sec_rows = iss_table_rows(data, "securities")
    si = {c: i for i, c in enumerate(scols)}
    meta: Dict[str, Any] = {}
    for row in sec_rows:
        sid = row[si["SECID"]]
        bid = row[si["BOARDID"]]
        if bid not in cfg.live_boards:
            continue
        if cfg.key == "stock" and bid != "TQBR":
            continue
        sn = row[si["SHORTNAME"]]
        ac = row[si["ASSETCODE"]] if "ASSETCODE" in si else None
        sen = row[si["SECNAME"]] if "SECNAME" in si else ""
        if cfg.key == "currency" and not is_currency_tom(sid):
            continue
        if cfg.key == "futures" and not is_futures_contract(str(sn), ac):
            continue
        if cfg.key == "currency":
            meta[sid] = {"SHORTNAME": sn, "SECNAME": sen}
        elif cfg.key == "futures":
            meta[sid] = {"SHORTNAME": sn, "SECNAME": sen, "ASSETCODE": ac}
        else:
            meta[sid] = {"SHORTNAME": sn, "SECNAME": sen}
    by_sec: Dict[str, Dict[str, Any]] = {}
    for row in md_rows:
        bid = row[mi["BOARDID"]]
        if bid not in cfg.live_boards:
            continue
        sid = row[mi["SECID"]]
        if sid not in meta:
            continue
        by_sec[sid] = {mcols[i]: row[i] for i in range(len(mcols))}
    return by_sec, meta


def current_price_live(md: Dict[str, Any]) -> Optional[float]:
    for key in ("MARKETPRICE", "LAST", "LCURRENTPRICE", "WAPRICE"):
        v = md.get(key)
        if v is not None:
            try:
                fv = float(v)
                if fv > 0:
                    return fv
            except (TypeError, ValueError):
                pass
    return None


def volume_rub_live(md: Dict[str, Any], engine: str) -> Optional[float]:
    for key in ("VALTODAY_RUR", "VALTODAY", "VALUE"):
        v = md.get(key)
        if v is not None:
            try:
                fv = float(v)
                if fv >= 0:
                    return fv
            except (TypeError, ValueError):
                pass
    return None


def build_table_rows(
    cfg: MarketConfig,
    mode: str,
    d0: str,
    d1: str,
    d2: str,
    stock_lim: Dict[str, float],
    cur_lim: Dict[str, float],
    fut_lim: Dict[str, float],
    cur_assets: List[str],
    vol_cache: Dict[str, Dict[str, float]],
    h1: Optional[Dict[str, Dict[str, Any]]] = None,
    h2: Optional[Dict[str, Dict[str, Any]]] = None,
    md_map: Optional[Dict[str, Dict[str, Any]]] = None,
    meta: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    h0 = load_history_cached(cfg, d0)
    if h1 is None:
        h1 = load_history_cached(cfg, d1)
    if h2 is None:
        h2 = load_history_cached(cfg, d2)

    fut_meta = load_futures_secmeta_for(h0.keys()) if cfg.key == "futures" else {}
    if cfg.key == "futures" and mode == "eod":
        prefetch_moex_futures_titles(
            [str(fut_meta.get(sid, {}).get("SHORTNAME") or h0[sid].get("SHORTNAME") or "") for sid in h0]
        )

    rows_out: List[Dict[str, Any]] = []

    if mode == "eod":
        for secid, r0 in h0.items():
            c0 = close_from_history_row(r0, cfg.engine)
            r1 = h1.get(secid)
            r2 = h2.get(secid)
            c1 = close_from_history_row(r1 or {}, cfg.engine) if r1 else None
            c2 = close_from_history_row(r2 or {}, cfg.engine) if r2 else None
            if c0 is None or c1 is None or c2 is None:
                continue
            ch1 = (c0 / c1 - 1.0) * 100.0
            ch2 = (c0 / c2 - 1.0) * 100.0
            assetcode = r0.get("ASSETCODE") if cfg.engine == "futures" else None
            hist_sn = r0.get("SHORTNAME") or secid
            fm = fut_meta.get(secid, {})
            if cfg.key == "futures":
                ticker = str(fm.get("SHORTNAME") or hist_sn)
                acode = str(fm.get("ASSETCODE") or assetcode or "").strip()
                disp_name = moex_futures_display_name(ticker)
            elif cfg.key == "currency":
                ticker = secid
                disp_name = ""
                acode = ""
            else:
                ticker = secid
                disp_name = str(hist_sn)
                acode = ""
            rk = risk_for_security(cfg, secid, assetcode, stock_lim, cur_lim, fut_lim, cur_assets)
            if risk_is_hundred_percent(rk):
                continue
            if cfg.key == "currency":
                vr = None
            else:
                vr = volume_rub_from_history(r0, cfg.engine)
            rp1 = ratio_pct(ch1, rk)
            rp2 = ratio_pct(ch2, rk)
            row_d: Dict[str, Any] = {
                "secid": secid,
                "ticker": ticker,
                "shortname": disp_name,
                "close0": round(c0, 6),
                "close1": round(c1, 6),
                "close2": round(c2, 6),
                "chg1": round(ch1, 4),
                "chg2": round(ch2, 4),
                "risk": round(rk * 100, 4) if rk is not None else None,
                "ratio1": round(rp1, 2) if rp1 is not None else None,
                "ratio2": round(rp2, 2) if rp2 is not None else None,
                "vol_rub": round(vr, 2) if vr is not None else None,
                "hl": row_highlight(rp1, rp2),
            }
            if cfg.key == "futures":
                row_d["asset_code"] = acode
            rows_out.append(row_d)
    else:
        assert md_map is not None and meta is not None
        if cfg.key == "futures":
            prefetch_moex_futures_titles(
                [str(meta.get(sid, {}).get("SHORTNAME") or "") for sid in md_map if meta.get(sid)]
            )
        for secid, md in md_map.items():
            m = meta.get(secid)
            if not m:
                continue
            c = current_price_live(md)
            if c is None:
                continue
            r1 = h1.get(secid)
            r2 = h2.get(secid)
            c1 = close_from_history_row(r1 or {}, cfg.engine) if r1 else None
            c2 = close_from_history_row(r2 or {}, cfg.engine) if r2 else None
            if c1 is None or c2 is None:
                continue
            ch1 = (c / c1 - 1.0) * 100.0
            ch2 = (c / c2 - 1.0) * 100.0
            assetcode = m.get("ASSETCODE")
            rk = risk_for_security(cfg, secid, assetcode, stock_lim, cur_lim, fut_lim, cur_assets)
            if risk_is_hundred_percent(rk):
                continue
            vr = None if cfg.key == "currency" else volume_rub_live(md, cfg.engine)
            rp1 = ratio_pct(ch1, rk)
            rp2 = ratio_pct(ch2, rk)
            if cfg.key == "futures":
                tkr = str(m.get("SHORTNAME") or secid)
                acode = str(m.get("ASSETCODE") or "").strip()
                disp_name = moex_futures_display_name(tkr)
            elif cfg.key == "currency":
                tkr = secid
                disp_name = ""
                acode = ""
            else:
                tkr = secid
                disp_name = str(m.get("SHORTNAME") or secid)
                acode = ""
            row_l: Dict[str, Any] = {
                "secid": secid,
                "ticker": tkr,
                "shortname": disp_name,
                "close0": round(c, 6),
                "close1": round(c1, 6),
                "close2": round(c2, 6),
                "chg1": round(ch1, 4),
                "chg2": round(ch2, 4),
                "risk": round(rk * 100, 4) if rk is not None else None,
                "ratio1": round(rp1, 2) if rp1 is not None else None,
                "ratio2": round(rp2, 2) if rp2 is not None else None,
                "vol_rub": round(vr, 2) if vr is not None else None,
                "hl": row_highlight(rp1, rp2),
            }
            if cfg.key == "futures":
                row_l["asset_code"] = acode
            rows_out.append(row_l)

    if cfg.key == "futures" and rows_out:
        annotate_futures_maturity_rank(rows_out)

    rows_out.sort(key=lambda x: -abs(x["chg1"]))
    return rows_out


def collect_eod_report_dates(stock_cfg: MarketConfig, today: date, max_sessions: int) -> List[str]:
    """Уникальные торговые даты отчёта (ed0), новые первыми — последняя завершённая сессия с ценами в истории."""
    out: List[str] = []
    seen: set = set()
    for delta in range(EOD_LOOKBACK_DAYS + 45):
        d = today - timedelta(days=delta)
        try:
            ed0, _, _ = get_three_trading_days(stock_cfg, d)
        except RuntimeError:
            continue
        if ed0 not in seen:
            seen.add(ed0)
            out.append(ed0)
        if len(out) >= max_sessions:
            break
    return out


def generate_report() -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    stock_lim = load_risk_limits_stock()
    cur_lim = load_risk_limits_currency()
    fut_lim = load_risk_limits_futures()
    cur_assets = sorted(cur_lim.keys(), key=len, reverse=True)
    vol_cache = merge_currency_volumes(load_volume_cache())

    today = date.today()
    stock_cfg = MARKETS["stock"]
    eod_report_dates = collect_eod_report_dates(stock_cfg, today, EOD_SESSIONS_IN_REPORT)
    eod_max_date = eod_report_dates[0] if eod_report_dates else today.strftime("%Y-%m-%d")
    eod_min_date = eod_report_dates[-1] if eod_report_dates else ""

    current_data: Dict[str, Any] = {}
    eod_data: Dict[str, Any] = {}

    for key, cfg in MARKETS.items():
        d0, d1, d2 = get_three_trading_days(cfg, today)
        h1 = load_history_cached(cfg, d1)
        h2 = load_history_cached(cfg, d2)
        md_map, meta = live_market_block(cfg)
        current_data[key] = {
            "title": cfg.title,
            "basis": f"Цены на момент запуска; база для Δ: закрытия {d1} и {d2}.",
            "rows": build_table_rows(
                cfg,
                "live",
                d0,
                d1,
                d2,
                stock_lim,
                cur_lim,
                fut_lim,
                cur_assets,
                vol_cache,
                h1=h1,
                h2=h2,
                md_map=md_map,
                meta=meta,
            ),
        }

        eod_by_date: Dict[str, Any] = {}
        for ed0 in eod_report_dates:
            try:
                ed0a, ed1, ed2 = get_three_trading_days(cfg, date.fromisoformat(ed0))
            except (RuntimeError, ValueError):
                continue
            if ed0a != ed0:
                continue
            try:
                rows = build_table_rows(
                    cfg,
                    "eod",
                    ed0,
                    ed1,
                    ed2,
                    stock_lim,
                    cur_lim,
                    fut_lim,
                    cur_assets,
                    vol_cache,
                )
            except Exception:
                continue
            eod_by_date[ed0] = {
                "trading_dates": {"report": ed0, "minus1d": ed1, "minus2d": ed2},
                "rows": rows,
            }
        eod_data[key] = {"title": cfg.title, "by_date": eod_by_date}

    payload = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "eod_max_date": eod_max_date,
        "eod_min_date": eod_min_date,
        "current": current_data,
        "eod": eod_data,
    }

    payload_json = json.dumps(payload, ensure_ascii=False)
    (SCRIPT_DIR / "moex_report_data.json").write_text(payload_json, encoding="utf-8")
    # В HTML нельзя вставлять сырой JSON с подстрокой "</..." — закроется <script>.
    # Экранируем как \u003c в JSON — JSON.parse вернёт корректные строки.
    embed_json = payload_json.replace("</", "\\u003c/")
    html = HTML_TEMPLATE.replace("__DATA__", embed_json)
    out_path = SCRIPT_DIR / "moex_report.html"
    out_path.write_text(html, encoding="utf-8")
    return out_path


HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Мониторинг достаточности ставок риска первого уровня</title>
  <script src="https://cdn.sheetjs.com/xlsx-0.20.2/package/dist/xlsx.full.min.js"></script>
  <style>
    :root {
      --bg: #fafafa;
      --btn: #c62828;
      --btn-hover: #9e1b1b;
      --text: #1a1a1a;
      --muted: #5c5c5c;
      --border: #e0e0e0;
    }
    * { box-sizing: border-box; }
    body {
      font-family: "Segoe UI", system-ui, sans-serif;
      margin: 0;
      background: var(--bg);
      color: var(--text);
      min-height: 100vh;
    }
    header {
      background: #fff;
      border-bottom: 1px solid var(--border);
      padding: 1rem 1.25rem;
    }
    h1 { font-size: 1.2rem; margin: 0 0 0.35rem; font-weight: 600; }
    .sub { font-size: 0.8rem; color: var(--muted); max-width: 900px; line-height: 1.4; }
    main { padding: 1rem 1.25rem 2rem; max-width: 1480px; margin: 0 auto; }
    .tabs, .mkt {
      display: flex; gap: 0.5rem; flex-wrap: wrap;
      margin-bottom: 0.75rem;
    }
    .tabs button, .mkt button {
      border: none;
      background: var(--btn);
      color: #fff;
      padding: 0.5rem 1rem;
      border-radius: 6px;
      cursor: pointer;
      font-size: 0.88rem;
      font-weight: 500;
    }
    .tabs button:hover, .mkt button:hover { background: var(--btn-hover); }
    .tabs button.off, .mkt button.off {
      background: #eee;
      color: #444;
    }
    .tabs button.off:hover, .mkt button.off:hover { background: #e0e0e0; }
    section.panel {
      background: #fff;
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 1rem;
      margin-bottom: 1rem;
    }
    h2 { font-size: 1rem; margin: 0 0 0.5rem; }
    .toolbar { display: flex; flex-wrap: wrap; gap: 0.75rem; align-items: center; margin-bottom: 0.75rem; }
    .toolbar input[type="search"], .toolbar input[type="date"], .toolbar select {
      padding: 0.4rem 0.5rem;
      border: 1px solid var(--border);
      border-radius: 6px;
      font-size: 0.88rem;
      background: #fff;
    }
    .toolbar label { font-size: 0.85rem; color: var(--muted); }
    .meta { font-size: 0.78rem; color: var(--muted); margin-bottom: 0.5rem; }
    .wrap { overflow-x: auto; }
    table { width: 100%; border-collapse: collapse; font-size: 0.8rem; }
    th, td { padding: 0.45rem 0.4rem; text-align: right; border-bottom: 1px solid var(--border); }
    th:first-child, td:first-child, th:nth-child(2), td:nth-child(2) { text-align: left; }
    th {
      position: sticky; top: 0;
      background: #f5f5f5;
      cursor: pointer;
      user-select: none;
      font-weight: 600;
      white-space: nowrap;
    }
    th:hover { background: #ececec; }
    th.sorted::after { content: " \\25b4"; font-size: 0.65em; opacity: 0.7; }
    th.sorted.desc::after { content: " \\25be"; }
    tr:hover td { background: #fafafa; }
    .hl-yellow { background: #fff9c4 !important; }
    .hl-red { background: #ffcdd2 !important; }
    .hl-crimson { background: #ef5350 !important; color: #1a0505; }
    .num-null { color: #bbb; }
    table.fx-cols th:nth-child(2),
    table.fx-cols td:nth-child(2) { text-align: right; }
    tr.col-filters td { text-align: center; background: #fafafa; padding: 0.25rem 0.2rem; }
    tr.col-filters input { width: 100%; max-width: 7rem; font-size: 0.72rem; padding: 0.2rem 0.25rem; border: 1px solid var(--border); border-radius: 4px; }
    tr.col-filters input.cf-numPair { max-width: 3.1rem; display: inline-block; }
    .export-btns { display: flex; gap: 0.5rem; flex-wrap: wrap; align-items: center; }
    .export-btns button {
      border: 1px solid var(--border); background: #fff; color: #333;
      padding: 0.35rem 0.65rem; border-radius: 6px; cursor: pointer; font-size: 0.82rem;
    }
    .export-btns button:hover { background: #f0f0f0; }
    .sync-hint { font-size: 0.75rem; color: var(--muted); }
  </style>
</head>
<body>
  <header>
    <h1>Мониторинг достаточности ставок риска первого уровня</h1>
    <p class="sub">
      <strong>Время обновления:</strong> <span id="gen"></span><br/><br/>
      В блоке «Текущие данные» при публикации на сайте данные подгружаются с сервера каждые 10 минут (в браузере). В офлайн-копии файла — снимок на момент последнего запуска скрипта.
      В блоке «Итоги торгового дня» — завершённые торговые сессии
      с официальными ценами закрытия. Доступны фондовый, валютный и срочный рынки (для фьючерсов можно отфильтровать контракты по ближайшим срокам экспирации).
      Инструменты с риском 100% в таблицу не попадают.
    </p>
  </header>
  <main>
    <div class="tabs">
      <button type="button" id="tab-cur" class="active">Текущие данные</button>
      <button type="button" id="tab-eod" class="off">Итоги торгового дня</button>
    </div>

    <section class="panel" id="panel-cur">
      <h2>Текущие данные</h2>
      <div class="mkt" id="mkt-cur">
        <button type="button" data-m="currency" class="off">Валютный</button>
        <button type="button" data-m="stock">Фондовый</button>
        <button type="button" data-m="futures" class="off">Срочный</button>
      </div>
      <div class="toolbar">
        <label>Поиск <input type="search" id="search-cur" placeholder="Тикер или название"/></label>
        <label id="wrap-fut-cur" style="display:none;">Срочность фьючерса
          <select id="fut-mat-cur">
            <option value="all" selected>Все фьючерсы</option>
            <option value="nearest">Ближайший по сроку</option>
            <option value="two">Два ближайших по сроку</option>
          </select>
        </label>
        <span class="export-btns">
          <button type="button" id="btn-csv-cur">Скачать CSV</button>
          <button type="button" id="btn-xlsx-cur">Скачать Excel</button>
        </span>
      </div>
      <p class="meta" id="meta-cur"></p>
      <p class="sync-hint" id="sync-hint-cur" style="display:none;"></p>
      <div class="wrap"><table id="tbl-cur"><thead></thead><tbody></tbody></table></div>
    </section>

    <section class="panel" id="panel-eod" style="display:none;">
      <h2>Итоги торгового дня</h2>
      <div class="mkt" id="mkt-eod">
        <button type="button" data-m="currency" class="off">Валютный</button>
        <button type="button" data-m="stock">Фондовый</button>
        <button type="button" data-m="futures" class="off">Срочный</button>
      </div>
      <div class="toolbar">
        <label>Дата <input type="date" id="date-eod"/></label>
        <label>Поиск <input type="search" id="search-eod" placeholder="Тикер или название"/></label>
        <label id="wrap-fut-eod" style="display:none;">Срочность фьючерса
          <select id="fut-mat-eod">
            <option value="all" selected>Все фьючерсы</option>
            <option value="nearest">Ближайший по сроку</option>
            <option value="two">Два ближайших по сроку</option>
          </select>
        </label>
        <span class="export-btns">
          <button type="button" id="btn-csv-eod">Скачать CSV</button>
          <button type="button" id="btn-xlsx-eod">Скачать Excel</button>
        </span>
      </div>
      <p class="meta" id="meta-eod"></p>
      <div class="wrap"><table id="tbl-eod"><thead></thead><tbody></tbody></table></div>
    </section>
  </main>
  <script type="application/json" id="moex-embedded-data">__DATA__</script>
  <script>
  function parseEmbeddedPayload() {
    const el = document.getElementById('moex-embedded-data');
    const raw = el ? el.textContent.trim() : '';
    if (!raw) return {};
    try {
      return JSON.parse(raw);
    } catch (e) {
      console.error('moex embedded JSON', e);
      return {};
    }
  }
  function ensureDataShape(d) {
    const emptyCur = () => ({ title: '', basis: '', rows: [] });
    const emptyEod = () => ({ title: '', by_date: {} });
    if (!d || typeof d !== 'object') d = {};
    if (!d.current || typeof d.current !== 'object') d.current = {};
    for (const k of ['stock', 'currency', 'futures']) {
      if (!d.current[k] || typeof d.current[k] !== 'object') d.current[k] = emptyCur();
      else if (!Array.isArray(d.current[k].rows)) d.current[k].rows = [];
    }
    if (!d.eod || typeof d.eod !== 'object') d.eod = {};
    for (const k of ['stock', 'currency', 'futures']) {
      if (!d.eod[k] || typeof d.eod[k] !== 'object') d.eod[k] = emptyEod();
      else if (!d.eod[k].by_date || typeof d.eod[k].by_date !== 'object') d.eod[k].by_date = {};
    }
    if (d.generated_at == null) d.generated_at = '';
    return d;
  }
  let DATA = ensureDataShape({});
  window.DATA_JSON_URL = window.DATA_JSON_URL || 'moex_report_data.json';
  const canPoll = window.location.protocol === 'http:' || window.location.protocol === 'https:';

  async function loadInitialData() {
    if (canPoll) {
      try {
        const url = (window.DATA_JSON_URL || 'moex_report_data.json') + '?t=' + Date.now();
        const r = await fetch(url, { cache: 'no-store' });
        if (r.ok) {
          DATA = ensureDataShape(await r.json());
          return;
        }
      } catch (e) {
        console.warn('fetch moex_report_data.json', e);
      }
    }
    DATA = ensureDataShape(parseEmbeddedPayload());
  }

  const COLS = [
    { key: 'ticker', label: 'Тикер', num: false },
    { key: 'shortname', label: 'Название', num: false },
    { key: 'close0', label: 'Цена', num: true },
    { key: 'close1', label: 'Цена закрытия −1д', num: true },
    { key: 'close2', label: 'Цена закрытия −2д', num: true },
    { key: 'chg1', label: 'Δ 1д %', num: true },
    { key: 'chg2', label: 'Δ 2д %', num: true },
    { key: 'risk', label: 'Ставка риска 1 ур.', num: true },
    { key: 'ratio1', label: '|Δ1|/СР1', num: true },
    { key: 'ratio2', label: '|Δ2|/СР1', num: true },
    { key: 'vol_rub', label: 'Оборот ₽', num: true },
  ];
  function colsForMarket(eod, market) {
    const pl = eod ? 'Цена закрытия' : 'Цена текущая';
    let base = COLS;
    if (market === 'currency') {
      base = COLS.filter(c => c.key !== 'shortname' && c.key !== 'vol_rub');
    }
    return base.map(c => (c.key === 'close0' ? Object.assign({}, c, { label: pl }) : c));
  }
  let mode = 'cur';
  let marketCur = 'stock';
  let marketEod = 'stock';
  let sortCol = 'chg1';
  let sortDir = -1;
  let sortColE = 'chg1';
  let sortDirE = -1;
  const filtersCurByMkt = { stock: {}, currency: {}, futures: {} };
  const filtersEodByMkt = { stock: {}, currency: {}, futures: {} };

  function updateGenLabel() {
    document.getElementById('gen').textContent = DATA.generated_at || '—';
  }

  function cellVal(r, c) {
    if (c.key === 'ticker') return r.ticker != null ? r.ticker : r.secid;
    return r[c.key];
  }
  function applyColFilters(rows, cols, filt) {
    return rows.filter(r => {
      for (const c of cols) {
        const slot = filt[c.key];
        if (!slot) continue;
        if (c.num) {
          const v = cellVal(r, c);
          const n = v != null && v !== '' ? Number(v) : null;
          const mn = slot.min !== '' && slot.min != null ? Number(slot.min) : null;
          const mx = slot.max !== '' && slot.max != null ? Number(slot.max) : null;
          if (mn != null && !isNaN(mn)) { if (n == null || isNaN(n) || n < mn) return false; }
          if (mx != null && !isNaN(mx)) { if (n == null || isNaN(n) || n > mx) return false; }
        } else {
          const q = (slot.text || '').trim().toLowerCase();
          if (!q) continue;
          const cell = String(cellVal(r, c) != null ? cellVal(r, c) : '').toLowerCase();
          if (!cell.includes(q)) return false;
        }
      }
      return true;
    });
  }
  function ensureSlot(filt, col) {
    if (!filt[col.key]) filt[col.key] = col.num ? { min: '', max: '' } : { text: '' };
    return filt[col.key];
  }

  function getRowsCur() {
    const b = DATA.current[marketCur];
    return (b && b.rows) ? b.rows.slice() : [];
  }
  function getRowsEod() {
    const d = document.getElementById('date-eod').value;
    const block = DATA.eod[marketEod];
    const day = block && block.by_date && block.by_date[d];
    return day && day.rows ? day.rows.slice() : [];
  }
  function filterRows(rows, q) {
    if (!q || !q.trim()) return rows;
    const s = q.trim().toLowerCase();
    return rows.filter(r =>
      String(r.ticker != null ? r.ticker : r.secid).toLowerCase().includes(s) ||
      String(r.secid || '').toLowerCase().includes(s) ||
      String(r.shortname || '').toLowerCase().includes(s)
    );
  }
  function applyFutMaturity(rows, mkt, selId) {
    if (mkt !== 'futures') return rows;
    const mode = (document.getElementById(selId) || {}).value || 'all';
    if (mode === 'all') return rows;
    return rows.filter(r => {
      const k = r.fut_rank;
      if (k == null) return true;
      if (mode === 'nearest') return k === 1;
      if (mode === 'two') return k <= 2;
      return true;
    });
  }
  function sortRows(rows, col, dir, colList) {
    const copy = rows.slice();
    const list = colList || COLS;
    const c = list.find(x => x.key === col);
    copy.sort((a, b) => {
      let va = a[col], vb = b[col];
      if (va == null && vb == null) return 0;
      if (va == null) return 1;
      if (vb == null) return -1;
      if (c && c.num) { va = Number(va); vb = Number(vb); }
      if (va < vb) return -dir;
      if (va > vb) return dir;
      return 0;
    });
    return copy;
  }
  function escapeAttr(s) {
    return String(s == null ? '' : s).replace(/&/g,'&amp;').replace(/"/g,'&quot;');
  }
  function renderThead(tblId, sortKey, sortD, onHeadClick, colList, filt, onFilterTyping) {
    const thead = document.querySelector('#' + tblId + ' thead');
    const cols = colList || COLS;
    cols.forEach(c => ensureSlot(filt, c));
    const row1 = '<tr>' + cols.map(c => {
      const cl = (c.key === sortKey) ? ' class="sorted' + (sortD < 0 ? ' desc' : '') + '"' : '';
      return '<th data-k="' + c.key + '"' + cl + '>' + c.label + '</th>';
    }).join('') + '</tr>';
    const row2 = '<tr class="col-filters">' + cols.map(c => {
      const s = filt[c.key];
      if (c.num) {
        const mn = (s && s.min) != null ? String(s.min) : '';
        const mx = (s && s.max) != null ? String(s.max) : '';
        return '<td onclick="event.stopPropagation()"><input type="number" step="any" class="cf-numPair" placeholder="≥" data-k="' + c.key + '" data-part="min" value="' + escapeAttr(mn) + '"/> ' +
          '<input type="number" step="any" class="cf-numPair" placeholder="≤" data-k="' + c.key + '" data-part="max" value="' + escapeAttr(mx) + '"/></td>';
      }
      const tv = (s && s.text) != null ? String(s.text) : '';
      return '<td onclick="event.stopPropagation()"><input type="text" class="cf-text" placeholder="содержит…" data-k="' + c.key + '" value="' + escapeAttr(tv) + '"/></td>';
    }).join('') + '</tr>';
    thead.innerHTML = row1 + row2;
    thead.querySelectorAll('th').forEach(th => {
      th.onclick = () => onHeadClick(th.dataset.k);
    });
    thead.querySelectorAll('tr.col-filters input').forEach(inp => {
      inp.oninput = () => {
        const k = inp.dataset.k;
        const col = cols.find(x => x.key === k);
        if (!col) return;
        const slot = ensureSlot(filt, col);
        if (col.num) {
          if (inp.dataset.part === 'min') slot.min = inp.value;
          else slot.max = inp.value;
        } else {
          slot.text = inp.value;
        }
        onFilterTyping();
      };
    });
  }
  function computeRowsCur() {
    let rows = getRowsCur();
    rows = applyFutMaturity(rows, marketCur, 'fut-mat-cur');
    rows = filterRows(rows, document.getElementById('search-cur').value);
    const ccols = colsForMarket(false, marketCur);
    const filt = filtersCurByMkt[marketCur];
    ccols.forEach(c => ensureSlot(filt, c));
    rows = applyColFilters(rows, ccols, filt);
    const keys = ccols.map(c => c.key);
    if (keys.indexOf(sortCol) < 0) { sortCol = 'chg1'; sortDir = -1; }
    rows = sortRows(rows, sortCol, sortDir, ccols);
    return { rows, ccols };
  }
  function computeRowsEod() {
    let rows = getRowsEod();
    rows = applyFutMaturity(rows, marketEod, 'fut-mat-eod');
    rows = filterRows(rows, document.getElementById('search-eod').value);
    const ecols = colsForMarket(true, marketEod);
    const filt = filtersEodByMkt[marketEod];
    ecols.forEach(c => ensureSlot(filt, c));
    rows = applyColFilters(rows, ecols, filt);
    const keysE = ecols.map(c => c.key);
    if (keysE.indexOf(sortColE) < 0) { sortColE = 'chg1'; sortDirE = -1; }
    rows = sortRows(rows, sortColE, sortDirE, ecols);
    return { rows, ecols };
  }
  function cellHtml(c, r, f, fmtNum, fmtVol) {
    const k = c.key;
    if (k === 'ticker') {
      const tick = (r.ticker != null && r.ticker !== '') ? r.ticker : r.secid;
      return '<td><strong>' + escapeHtml(String(tick)) + '</strong></td>';
    }
    if (k === 'shortname') return '<td>' + escapeHtml(String(r.shortname || '')) + '</td>';
    if (k === 'close0' || k === 'close1' || k === 'close2' || k === 'chg1' || k === 'chg2' || k === 'risk' || k === 'ratio1' || k === 'ratio2')
      return '<td>' + fmtNum(r[k]) + '</td>';
    if (k === 'vol_rub') return '<td>' + fmtVol(r.vol_rub) + '</td>';
    return '<td></td>';
  }
  function renderTbody(tbody, rows, colList) {
    const cols = colList || COLS;
    tbody.innerHTML = rows.map(r => {
      const f = (x) => (x != null && x !== '') ? x : '<span class="num-null">—</span>';
      const fmtNum = (x) => (x != null && typeof x === 'number') ? x.toLocaleString('ru-RU', { maximumFractionDigits: 4 }) : f(x);
      const fmtVol = (x) => (x != null && typeof x === 'number') ? x.toLocaleString('ru-RU', { maximumFractionDigits: 0 }) : f(x);
      return '<tr class="' + (r.hl || '') + '">' + cols.map(c => cellHtml(c, r, f, fmtNum, fmtVol)).join('') + '</tr>';
    }).join('');
  }
  function escapeHtml(s) {
    return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  }
  function refreshCurBodyOnly() {
    const { rows, ccols } = computeRowsCur();
    renderTbody(document.querySelector('#tbl-cur tbody'), rows, ccols);
  }
  function refreshEodBodyOnly() {
    const { rows, ecols } = computeRowsEod();
    renderTbody(document.querySelector('#tbl-eod tbody'), rows, ecols);
  }
  function refreshCur() {
    if (!DATA || !DATA.current) return;
    const inp = document.getElementById('search-cur');
    inp.placeholder = (marketCur === 'currency') ? 'Тикер' : 'Тикер или название';
    document.getElementById('wrap-fut-cur').style.display = (marketCur === 'futures') ? '' : 'none';
    const { rows, ccols } = computeRowsCur();
    document.getElementById('meta-cur').textContent = (DATA.current[marketCur] && DATA.current[marketCur].basis) || '';
    document.getElementById('tbl-cur').className = (marketCur === 'currency') ? 'fx-cols' : '';
    const filt = filtersCurByMkt[marketCur];
    renderThead('tbl-cur', sortCol, sortDir, (k) => {
      if (k === sortCol) sortDir = -sortDir; else { sortCol = k; sortDir = -1; }
      refreshCur();
    }, ccols, filt, refreshCurBodyOnly);
    renderTbody(document.querySelector('#tbl-cur tbody'), rows, ccols);
  }
  function refreshEod() {
    if (!DATA || !DATA.eod) return;
    const inpe = document.getElementById('search-eod');
    inpe.placeholder = (marketEod === 'currency') ? 'Тикер' : 'Тикер или название';
    document.getElementById('wrap-fut-eod').style.display = (marketEod === 'futures') ? '' : 'none';
    const { rows, ecols } = computeRowsEod();
    const d = document.getElementById('date-eod').value;
    const block = DATA.eod[marketEod];
    const day = block && block.by_date && block.by_date[d];
    const td = day && day.trading_dates;
    document.getElementById('meta-eod').textContent = td
      ? ('Торговые дни: отчёт ' + td.report + ', −1д ' + td.minus1d + ', −2д ' + td.minus2d)
      : ('Нет данных за ' + d);
    document.getElementById('tbl-eod').className = (marketEod === 'currency') ? 'fx-cols' : '';
    const filt = filtersEodByMkt[marketEod];
    renderThead('tbl-eod', sortColE, sortDirE, (k) => {
      if (k === sortColE) sortDirE = -sortDirE; else { sortColE = k; sortDirE = -1; }
      refreshEod();
    }, ecols, filt, refreshEodBodyOnly);
    renderTbody(document.querySelector('#tbl-eod tbody'), rows, ecols);
  }
  function rowsToAoA(rows, cols) {
    const head = cols.map(c => c.label);
    const data = rows.map(r => cols.map(c => {
      const v = cellVal(r, c);
      return v != null && v !== '' ? v : '';
    }));
    return [head].concat(data);
  }
  function downloadBlob(filename, mime, text) {
    const a = document.createElement('a');
    a.href = URL.createObjectURL(new Blob([text], { type: mime }));
    a.download = filename;
    a.click();
    URL.revokeObjectURL(a.href);
  }
  function exportCsv(filename, rows, cols) {
    const aoa = rowsToAoA(rows, cols);
    const sep = ';';
    const lines = aoa.map(row => row.map(cell => {
      const s = String(cell).replace(/"/g, '""');
      if (/[;"\\n\\r]/.test(s)) return '"' + s + '"';
      return s;
    }).join(sep));
    downloadBlob(filename, 'text/csv;charset=utf-8', String.fromCharCode(0xFEFF) + lines.join('\\r\\n'));
  }
  function exportXlsx(filename, rows, cols) {
    if (typeof XLSX === 'undefined') {
      alert('Библиотека Excel ещё загружается. Повторите через секунду или используйте CSV.');
      return;
    }
    const aoa = rowsToAoA(rows, cols);
    const ws = XLSX.utils.aoa_to_sheet(aoa);
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, 'Sheet1');
    XLSX.writeFile(wb, filename);
  }
  function safeFilename(s) {
    return String(s).replace(/[^a-zA-Z0-9._-]+/g, '_').slice(0, 80);
  }
  document.getElementById('tab-cur').onclick = () => {
    document.getElementById('tab-cur').className = 'active';
    document.getElementById('tab-eod').className = 'off';
    document.getElementById('panel-cur').style.display = 'block';
    document.getElementById('panel-eod').style.display = 'none';
  };
  document.getElementById('tab-eod').onclick = () => {
    document.getElementById('tab-eod').className = 'active';
    document.getElementById('tab-cur').className = 'off';
    document.getElementById('panel-eod').style.display = 'block';
    document.getElementById('panel-cur').style.display = 'none';
  };
  function bindMkt(id, isEod) {
    document.querySelectorAll('#' + id + ' button').forEach(btn => {
      btn.onclick = () => {
        document.querySelectorAll('#' + id + ' button').forEach(b => { b.className = 'off'; });
        btn.className = '';
        if (isEod) { marketEod = btn.dataset.m; refreshEod(); }
        else { marketCur = btn.dataset.m; refreshCur(); }
      };
    });
  }
  bindMkt('mkt-cur', false);
  bindMkt('mkt-eod', true);
  document.getElementById('search-cur').oninput = refreshCurBodyOnly;
  document.getElementById('search-eod').oninput = refreshEodBodyOnly;
  document.getElementById('fut-mat-cur').onchange = refreshCur;
  document.getElementById('fut-mat-eod').onchange = refreshEod;

  const hint = document.getElementById('sync-hint-cur');
  if (hint) {
    hint.style.display = canPoll ? 'block' : 'none';
    hint.textContent = canPoll ? 'Раздел «Текущие данные» автоматически подгружает свежий файл данных каждые 10 минут (пока страница открыта по HTTP/HTTPS).' : '';
  }
  async function pullLatestData() {
    if (!canPoll) return;
    try {
      const url = (window.DATA_JSON_URL || 'moex_report_data.json') + '?t=' + Date.now();
      const r = await fetch(url, { cache: 'no-store' });
      if (!r.ok) return;
      const j = await r.json();
      DATA = ensureDataShape(j);
      updateGenLabel();
      refreshCur();
      refreshEod();
    } catch (e) { console.warn('moex_report_data.json', e); }
  }
  setInterval(pullLatestData, 10 * 60 * 1000);
  document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'visible' && canPoll) pullLatestData();
  });

  function stamp() { return safeFilename((DATA.generated_at || 'export').replace(/:/g, '-')); }
  document.getElementById('btn-csv-cur').onclick = () => {
    const { rows, ccols } = computeRowsCur();
    exportCsv('current_' + marketCur + '_' + stamp() + '.csv', rows, ccols);
  };
  document.getElementById('btn-xlsx-cur').onclick = () => {
    const { rows, ccols } = computeRowsCur();
    exportXlsx('current_' + marketCur + '_' + stamp() + '.xlsx', rows, ccols);
  };
  document.getElementById('btn-csv-eod').onclick = () => {
    const { rows, ecols } = computeRowsEod();
    const d = document.getElementById('date-eod').value || '';
    exportCsv('eod_' + marketEod + '_' + d + '_' + stamp() + '.csv', rows, ecols);
  };
  document.getElementById('btn-xlsx-eod').onclick = () => {
    const { rows, ecols } = computeRowsEod();
    const d = document.getElementById('date-eod').value || '';
    exportXlsx('eod_' + marketEod + '_' + d + '_' + stamp() + '.xlsx', rows, ecols);
  };

  function syncEodDateInput() {
    const inp = document.getElementById('date-eod');
    const eodMax = DATA.eod_max_date || '';
    const eodMin = DATA.eod_min_date || '';
    if (eodMax) {
      inp.max = eodMax;
      inp.value = eodMax;
    } else {
      const maxD = new Date();
      inp.max = maxD.toISOString().slice(0, 10);
      inp.value = maxD.toISOString().slice(0, 10);
    }
    if (eodMin) {
      inp.min = eodMin;
    } else {
      const minD = new Date();
      minD.setDate(minD.getDate() - 30);
      inp.min = minD.toISOString().slice(0, 10);
    }
    inp.onchange = refreshEod;
  }

  async function startApp() {
    document.getElementById('gen').textContent = canPoll ? 'Загрузка…' : (DATA.generated_at || '—');
    await loadInitialData();
    updateGenLabel();
    syncEodDateInput();
    refreshCur();
    refreshEod();
  }
  startApp();
  </script>
</body>
</html>
"""


def main() -> None:
    path = generate_report()
    uri = path.as_uri()
    print("Отчёт сохранён:", path)
    print("Данные для автообновления:", SCRIPT_DIR / "moex_report_data.json")
    try:
        os.startfile(path)
    except AttributeError:
        webbrowser.open(uri)


if __name__ == "__main__":
    main()
