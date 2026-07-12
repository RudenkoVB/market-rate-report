#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Отчёт MOEX: цены и ставки риска 1-го уровня (публичный ISS API).

Запуск: python moex_risk_monitor.py
Создаются moex_report.html и moex_report_data.json в этой папке; открывается HTML в браузере.

Ставки риска 1 ур.: публичные таблицы RMS MOEX ISS (im1 для фондового и валютного рынков,
mr1 для срочного) — те же значения, что публикуются в статических риск-параметрах НКЦ
(https://www.nationalclearingcentre.ru/), в машиночитаемом виде доступны через ISS.
Кэш истории: папка moex_cache/; объёмы валют по датам — currency_volume_by_date.json;
медианы оборотов для лимитов концентрации — conc_vol_medians_v2.json.

«Лимиты концентрации»: расчётный ЛК по медианам дневного оборота (год / 3 мес.),
текущие ЛК — limit1/limit2 (фондовый) и lk1/lk2 (срочный) из RMS ISS (НКЦ).

«Текущие данные»: таблица marketdata ISS — в первую очередь поле LAST (цена последней сделки на момент
запроса); при отсутствии сделок — лучшие BID/OFFER из стакана; далее LCURRENTPRICE и др. Запрос без кэша.
"""

from __future__ import annotations

import json
import math
import os
import re
import socket
import time
import urllib.error
import urllib.parse
import urllib.request
import webbrowser
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, Iterable, List, Optional, Tuple

# МСК = UTC+3 (в РФ постоянно, без перевода часов).
MSK_TZ = timezone(timedelta(hours=3))

ISS = "https://iss.moex.com/iss"
PAGESIZE = 100
EOD_LOOKBACK_DAYS = 45
# Итоги торгового дня: календарное окно «последний месяц» для выбора даты.
EOD_MONTH_CALENDAR_DAYS = 31
# ~1 календарный месяц в торговых сессиях для колонки «1м, %».
TRADING_SESSIONS_1M = 21

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


def fetch_json(url: str, timeout: float = 120.0, retries: int = 4) -> Any:
    """Запрос к ISS; при таймаутах и временной недоступности MOEX — повтор с паузой."""
    last_err: Optional[BaseException] = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "moex-risk-monitor/2.0"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            last_err = e
            if e.code in (429, 502, 503, 504) and attempt < retries - 1:
                time.sleep(min(10.0, 2.0**attempt))
                continue
            raise
        except (TimeoutError, socket.timeout) as e:
            last_err = e
            if attempt < retries - 1:
                time.sleep(min(10.0, 2.0**attempt))
                continue
            raise
        except (urllib.error.URLError, ConnectionError, BrokenPipeError, OSError) as e:
            last_err = e
            if attempt < retries - 1:
                time.sleep(min(10.0, 2.0**attempt))
                continue
            raise
    assert last_err is not None
    raise last_err


def fetch_json_live(url: str, timeout: float = 60.0, retries: int = 2) -> Any:
    """Запрос снимка marketdata для «Текущих данных»: тот же ISS, без кэширования, актуальная сессия."""
    last_err: Optional[BaseException] = None
    for attempt in range(retries):
        sep = "&" if "?" in url else "?"
        url_bust = f"{url}{sep}_t={time.time_ns()}"
        try:
            req = urllib.request.Request(
                url_bust,
                headers={
                    "User-Agent": "moex-risk-monitor/2.1-live",
                    "Cache-Control": "no-cache, no-store",
                    "Pragma": "no-cache",
                },
            )
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            last_err = e
            if e.code in (429, 502, 503, 504) and attempt < retries - 1:
                time.sleep(min(3.0, 1.3**attempt))
                continue
            raise
        except (TimeoutError, socket.timeout) as e:
            last_err = e
            if attempt < retries - 1:
                time.sleep(min(3.0, 1.3**attempt))
                continue
            raise
        except (urllib.error.URLError, ConnectionError, BrokenPipeError, OSError) as e:
            last_err = e
            if attempt < retries - 1:
                time.sleep(min(3.0, 1.3**attempt))
                continue
            raise
    assert last_err is not None
    raise last_err


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
    risk, _ = load_stock_limits_combined()
    return risk


def load_stock_limits_combined() -> Tuple[Dict[str, float], Dict[str, Dict[str, Any]]]:
    """Один запрос RMS: im1 для мониторинга риска и limit1/limit2 для лимитов концентрации."""
    rows = paginate_iss("/rms/engines/stock/objects/limits.json", "limits")
    risk: Dict[str, float] = {}
    conc: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        if len(r) < 8:
            continue
        secid = str(r[1])
        if str(r[2]) != "SUR" or secid.endswith("-RM"):
            continue
        try:
            im1 = float(r[3])
            risk[secid] = im1 / 100.0
            if im1 >= 100:
                continue
            conc[secid] = {"limit1": float(r[6]), "limit2": float(r[7]), "im1": im1}
        except (TypeError, ValueError):
            continue
    return risk, conc


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
    risk, _ = load_futures_limits_combined()
    return risk


def load_futures_limits_combined() -> Tuple[Dict[str, float], Dict[str, Dict[str, Any]]]:
    """Один запрос RMS: mr1 и lk1/lk2 (как на сайте НКЦ)."""
    rows = paginate_iss("/rms/engines/futures/objects/limits.json", "limits")
    risk: Dict[str, float] = {}
    conc: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        if len(r) < 9:
            continue
        asset = str(r[1])
        try:
            mr1 = float(r[2])
            risk[asset] = mr1
            if mr1 >= 1.0 - 1e-9:
                continue
            conc[asset] = {
                "lk1": float(r[5]),
                "lk2": float(r[6]),
                "mr1": mr1,
                "title": str(r[7] or ""),
                "group_title": str(r[8] or ""),
            }
        except (TypeError, ValueError):
            continue
    return risk, conc


CONC_YEAR_CALENDAR_DAYS = 400
CONC_3M_CALENDAR_DAYS = 110
CONC_MEDIAN_CACHE_FILE = CACHE_DIR / f"conc_vol_medians_{CACHE_VER}.json"

_CALENDAR_CACHE: Dict[str, List[str]] = {}


def load_concentration_limits_stock() -> Dict[str, Dict[str, Any]]:
    _, conc = load_stock_limits_combined()
    return conc


def load_concentration_limits_futures() -> Dict[str, Dict[str, Any]]:
    _, conc = load_futures_limits_combined()
    return conc


def futures_expiry_date(shortname: str) -> Optional[date]:
    exp = parse_futures_expiry_mmyy(shortname)
    if not exp:
        return None
    year, mon = exp
    from calendar import monthrange

    return date(year, mon, monthrange(year, mon)[1])


def adjusted_price(
    price: Optional[float],
    lot_volume: float,
    min_step: float,
    step_price: Optional[float],
) -> Optional[float]:
    if price is None or price <= 0 or lot_volume <= 0 or min_step <= 0:
        return None
    sp = step_price if step_price is not None and step_price > 0 else min_step
    return (price * sp) / (lot_volume * min_step)


def stat_median(values: List[float]) -> Optional[float]:
    vals = sorted(v for v in values if v is not None and v > 0)
    if not vals:
        return None
    n = len(vals)
    mid = n // 2
    if n % 2:
        return vals[mid]
    return (vals[mid - 1] + vals[mid]) / 2.0


def extended_trading_dates(cfg: MarketConfig, end: date, calendar_lookback: int) -> List[str]:
    if cfg.key == "futures":
        sample = pick_futures_sample_sec()
    elif cfg.key == "currency":
        sample = "CNYRUB_TOM"
    else:
        sample = "SBER"
    frm = (end - timedelta(days=calendar_lookback)).strftime("%Y-%m-%d")
    till = end.strftime("%Y-%m-%d")
    path = f"/history/engines/{cfg.engine}/markets/{cfg.market}/securities/{sample}.json"
    dates: set = set()
    start = 0
    while True:
        q = urllib.parse.urlencode({"from": frm, "till": till, "iss.meta": "off", "start": str(start), "limit": "100"})
        data = fetch_json(f"{ISS}{path}?{q}")
        _, rows = iss_table_rows(data, "history")
        cols = data.get("history", {}).get("columns") or []
        if not cols or not rows:
            break
        idx = {c: i for i, c in enumerate(cols)}
        for row in rows:
            td = row[idx["TRADEDATE"]]
            if td <= till:
                dates.add(td)
        start += 100
        if len(rows) < 100:
            break
    return sorted(dates)


def pick_front_futures_by_asset(hist_by_sec: Dict[str, Dict[str, Any]], trade_day: date) -> Dict[str, Dict[str, Any]]:
    grouped: Dict[str, List[Tuple[date, Dict[str, Any]]]] = defaultdict(list)
    for rec in hist_by_sec.values():
        sn = str(rec.get("SHORTNAME") or "")
        ac = rec.get("ASSETCODE")
        if not is_futures_contract(sn, str(ac) if ac is not None else None):
            continue
        exp_d = futures_expiry_date(sn)
        if exp_d is None or exp_d < trade_day:
            continue
        grouped[str(ac)].append((exp_d, rec))
    out: Dict[str, Dict[str, Any]] = {}
    for ac, items in grouped.items():
        out[ac] = min(items, key=lambda x: (x[0], str(x[1].get("SECID") or "")))[1]
    return out


def load_stock_lot_specs(secids: Iterable[str]) -> Dict[str, Dict[str, float]]:
    want = sorted({str(s) for s in secids})
    out: Dict[str, Dict[str, float]] = {}
    batch = 50
    for i in range(0, len(want), batch):
        chunk = want[i : i + batch]
        q = urllib.parse.urlencode(
            {
                "iss.meta": "off",
                "securities": ",".join(chunk),
                "securities.columns": "SECID,LOTSIZE,MINSTEP,BOARDID",
            }
        )
        data = fetch_json(f"{ISS}/engines/stock/markets/shares/securities.json?{q}")
        cols = data.get("securities", {}).get("columns") or []
        _, rows = iss_table_rows(data, "securities")
        if not cols:
            continue
        si = {c: j for j, c in enumerate(cols)}
        for row in rows:
            if "BOARDID" in si and row[si["BOARDID"]] != "TQBR":
                continue
            sid = str(row[si["SECID"]])
            try:
                lot = float(row[si["LOTSIZE"]]) if "LOTSIZE" in si else 1.0
                step = float(row[si["MINSTEP"]]) if "MINSTEP" in si else 0.01
            except (TypeError, ValueError):
                lot, step = 1.0, 0.01
            out[sid] = {"lot_volume": lot if lot > 0 else 1.0, "min_step": step if step > 0 else 0.01}
    return out


def load_futures_lot_specs(secids: Iterable[str]) -> Dict[str, Dict[str, float]]:
    store = load_futures_secmeta_for(secids)
    out: Dict[str, Dict[str, float]] = {}
    batch = 40
    want = sorted({str(s) for s in secids})
    for i in range(0, len(want), batch):
        chunk = want[i : i + batch]
        q = urllib.parse.urlencode(
            {
                "iss.meta": "off",
                "securities": ",".join(chunk),
                "securities.columns": "SECID,LOTVOLUME,MINSTEP,STEPPRICE",
            }
        )
        data = fetch_json(f"{ISS}/engines/futures/markets/forts/securities.json?{q}")
        cols = data.get("securities", {}).get("columns") or []
        _, rows = iss_table_rows(data, "securities")
        if not cols:
            continue
        si = {c: j for j, c in enumerate(cols)}
        for row in rows:
            sid = str(row[si["SECID"]])
            try:
                out[sid] = {
                    "lot_volume": max(float(row[si["LOTVOLUME"]]), 1.0),
                    "min_step": max(float(row[si["MINSTEP"]]), 1e-9),
                    "step_price": max(float(row[si["STEPPRICE"]]), 1e-9),
                }
            except (TypeError, ValueError):
                meta = store.get(sid, {})
                out[sid] = {"lot_volume": 1.0, "min_step": 1.0, "step_price": 1.0}
    return out


def _load_or_build_volume_medians(report_day: str, today: date) -> Tuple[Dict[str, Dict[str, float]], Dict[str, Dict[str, float]]]:
    if CONC_MEDIAN_CACHE_FILE.is_file():
        with open(CONC_MEDIAN_CACHE_FILE, "r", encoding="utf-8") as f:
            cached = json.load(f)
        if cached.get("report_day") == report_day:
            return cached.get("stock") or {}, cached.get("futures") or {}

    stock_cfg = MARKETS["stock"]
    fut_cfg = MARKETS["futures"]
    year_dates = extended_trading_dates(stock_cfg, date.fromisoformat(report_day), CONC_YEAR_CALENDAR_DAYS)
    if report_day not in year_dates:
        year_dates = [d for d in year_dates if d <= report_day]
    three_m_cutoff = (
        date.fromisoformat(report_day) - timedelta(days=CONC_3M_CALENDAR_DAYS)
    ).strftime("%Y-%m-%d")
    dates_3m = [d for d in year_dates if d >= three_m_cutoff]

    stock_vols: Dict[str, List[float]] = defaultdict(list)
    stock_vols_3m: Dict[str, List[float]] = defaultdict(list)
    fut_vols: Dict[str, List[float]] = defaultdict(list)
    fut_vols_3m: Dict[str, List[float]] = defaultdict(list)

    def load_day_pair(d: str) -> None:
        sh = load_history_cached(stock_cfg, d)
        for secid, rec in sh.items():
            vr = volume_rub_from_history(rec, stock_cfg.engine)
            if vr is not None and vr > 0:
                stock_vols[secid].append(vr)
                if d in dates_3m:
                    stock_vols_3m[secid].append(vr)
        fh = load_history_cached(fut_cfg, d)
        front = pick_front_futures_by_asset(fh, date.fromisoformat(d))
        for ac, rec in front.items():
            vr = volume_rub_from_history(rec, fut_cfg.engine)
            if vr is not None and vr > 0:
                fut_vols[ac].append(vr)
                if d in dates_3m:
                    fut_vols_3m[ac].append(vr)

    with ThreadPoolExecutor(max_workers=16) as ex:
        list(ex.map(load_day_pair, year_dates))

    stock_medians = {
        sid: {"med_year": stat_median(stock_vols[sid]), "med_3m": stat_median(stock_vols_3m.get(sid, []))}
        for sid in stock_vols
    }
    fut_medians = {
        ac: {"med_year": stat_median(fut_vols[ac]), "med_3m": stat_median(fut_vols_3m.get(ac, []))}
        for ac in fut_vols
    }
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with open(CONC_MEDIAN_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(
            {"report_day": report_day, "stock": stock_medians, "futures": fut_medians},
            f,
            ensure_ascii=False,
        )
    return stock_medians, fut_medians


def calc_lk_from_volumes(med_year: Optional[float], med_3m: Optional[float]) -> Tuple[Optional[float], Optional[float]]:
    parts = [p for p in (med_year, med_3m) if p is not None and p > 0]
    if not parts:
        return None, None
    my = med_year or med_3m or 0.0
    m3 = med_3m or med_year or 0.0
    lk2_rub = 0.2 * my + 0.8 * m3
    lk1_rub = lk2_rub / 5.0
    return lk1_rub, lk2_rub


def futures_lk_to_rub(lk: float, ba_price: Optional[float]) -> Optional[float]:
    """ЛК в единицах БА → рубли: lk × цена БА."""
    if ba_price is None or ba_price <= 0:
        return None
    return lk * ba_price


def conc_row_highlight(delta: Optional[float]) -> str:
    if delta is None or math.isnan(delta):
        return ""
    if abs(delta) <= 10:
        return ""
    if delta > 10:
        return "hl-green"
    return "hl-yellow-conc"


def _conc_row(
    ticker: str,
    shortname: str,
    price: Optional[float],
    lk1_cur: Optional[float],
    lk2_cur: Optional[float],
    lk1_calc: Optional[float],
    lk2_calc: Optional[float],
    lk2_cur_rub: Optional[float],
    lk2_calc_rub: Optional[float],
) -> Dict[str, Any]:
    delta: Optional[float] = None
    if lk2_cur_rub is not None and lk2_cur_rub > 0 and lk2_calc_rub is not None:
        delta = (lk2_calc_rub - lk2_cur_rub) / lk2_cur_rub * 100.0
    return {
        "ticker": ticker,
        "shortname": shortname,
        "price": round_price_display(price),
        "lk1_cur": round(lk1_cur, 0) if lk1_cur is not None else None,
        "lk2_cur": round(lk2_cur, 0) if lk2_cur is not None else None,
        "lk1_calc": round(lk1_calc, 0) if lk1_calc is not None else None,
        "lk2_calc": round(lk2_calc, 0) if lk2_calc is not None else None,
        "lk2_cur_rub": round(lk2_cur_rub, 0) if lk2_cur_rub is not None else None,
        "lk2_calc_rub": round(lk2_calc_rub, 0) if lk2_calc_rub is not None else None,
        "lk2_delta_pct": round(delta, 1) if delta is not None else None,
        "hl": conc_row_highlight(delta),
    }


def build_concentration_data(
    report_day: str,
    stock_conc: Dict[str, Dict[str, Any]],
    fut_conc: Dict[str, Dict[str, Any]],
    stock_medians: Dict[str, Dict[str, float]],
    fut_medians: Dict[str, Dict[str, float]],
) -> Dict[str, Any]:
    stock_cfg = MARKETS["stock"]
    fut_cfg = MARKETS["futures"]
    trade_d = date.fromisoformat(report_day)

    stock_hist = load_history_cached(stock_cfg, report_day)
    fut_hist = load_history_cached(fut_cfg, report_day)
    front = pick_front_futures_by_asset(fut_hist, trade_d)

    fut_specs = load_futures_lot_specs([rec.get("SECID") for rec in front.values() if rec.get("SECID")])

    stock_rows: List[Dict[str, Any]] = []
    for secid, lim in stock_conc.items():
        rec = stock_hist.get(secid)
        if not rec:
            continue
        price = close_from_history_row(rec, stock_cfg.engine)
        if price is None or price <= 0:
            continue
        meds = stock_medians.get(secid, {})
        lk1_rub, lk2_rub = calc_lk_from_volumes(meds.get("med_year"), meds.get("med_3m"))
        if lk2_rub is None:
            continue
        lk1_calc = lk1_rub / price if lk1_rub else None
        lk2_calc = lk2_rub / price
        l1c = lim["limit1"]
        l2c = lim["limit2"]
        stock_rows.append(
            _conc_row(
                secid,
                str(rec.get("SHORTNAME") or secid),
                price,
                l1c,
                l2c,
                lk1_calc,
                lk2_calc,
                l2c * price,
                lk2_rub,
            )
        )

    fut_rows: List[Dict[str, Any]] = []
    for ac, lim in fut_conc.items():
        if lim.get("group_title") == "Акции":
            continue
        rec = front.get(ac)
        if not rec:
            continue
        secid = str(rec.get("SECID") or "")
        settle = close_from_history_row(rec, fut_cfg.engine)
        spec = fut_specs.get(secid, {"lot_volume": 1.0, "min_step": 1.0, "step_price": 1.0})
        ba_price = adjusted_price(
            settle, spec["lot_volume"], spec["min_step"], spec["step_price"]
        )
        if ba_price is None:
            continue
        meds = fut_medians.get(ac, {})
        lk1_rub, lk2_rub = calc_lk_from_volumes(meds.get("med_year"), meds.get("med_3m"))
        if lk2_rub is None:
            continue
        lk1_calc = lk1_rub / ba_price if lk1_rub else None
        lk2_calc = lk2_rub / ba_price
        l1c = lim["lk1"]
        l2c = lim["lk2"]
        lk2_cur_rub = futures_lk_to_rub(l2c, ba_price)
        disp = (lim.get("title") or ac).strip()
        fut_rows.append(
            _conc_row(
                ac,
                disp,
                ba_price,
                l1c,
                l2c,
                lk1_calc,
                lk2_calc,
                lk2_cur_rub,
                lk2_rub,
            )
        )

    stock_rows.sort(key=lambda r: abs(r.get("lk2_delta_pct") or 0), reverse=True)
    fut_rows.sort(key=lambda r: abs(r.get("lk2_delta_pct") or 0), reverse=True)
    return {
        "report_day": report_day,
        "stock": {"title": "Фондовый рынок", "rows": stock_rows},
        "futures": {"title": "Срочный рынок", "rows": fut_rows},
    }


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


def _trading_dates_up_to(cfg: MarketConfig, report_end: date) -> List[str]:
    """Уникальные TRADEDATE из ISS history по одному инструменту, по возрастанию."""
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
    return dates


def get_three_trading_days(cfg: MarketConfig, report_end: date) -> Tuple[str, str, str]:
    dates = _trading_dates_up_to(cfg, report_end)
    return dates[-1], dates[-2], dates[-3]


def live_comparison_close_dates(cfg: MarketConfig, today: date) -> Tuple[str, str]:
    """
    Даты официальных закрытий для Δ1д и Δ2д в блоке «Текущие данные»:
    предыдущий и позапрошлый торговые дни относительно календарной даты отчёта.

    Если в ISS уже есть строка TRADEDATE на «сегодня», последняя в списке — текущая сессия;
    тогда закрытие «вчера» — dates[-2], «позавчера (в торгах)» — dates[-3].
    Если сегодняшнего дня в истории ещё нет, последняя дата — вчера → «вчера» dates[-1], позавчера dates[-2].
    """
    dates = _trading_dates_up_to(cfg, today)
    today_s = today.strftime("%Y-%m-%d")
    if dates[-1] == today_s:
        return dates[-2], dates[-3]
    return dates[-1], dates[-2]


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


def round_price_display(value: Optional[float]) -> Optional[float]:
    """Округление цены: обычно до сотых; для цен < 0.1 — до второго значащего дробного знака."""
    if value is None:
        return None
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(v):
        return None
    av = abs(v)
    if av == 0:
        return 0.0
    if av >= 0.1:
        return round(v, 2)
    pos = 0
    for i in range(1, 15):
        if av * (10**i) >= 1:
            pos = i
            break
    else:
        pos = 14
    return round(v, pos + 1)


def row_highlight(r1: Optional[float], r2: Optional[float]) -> str:
    vals = [x for x in (r1, r2) if x is not None and not math.isnan(x)]
    if not vals:
        return ""
    m = max(vals)
    if m >= 100:
        return "hl-red"
    if m >= 80:
        return "hl-yellow"
    return ""


def trading_dates_cached(cfg: MarketConfig, end: date, lookback: int = EOD_LOOKBACK_DAYS + 20) -> List[str]:
    key = f"{cfg.key}:{end.isoformat()}:{lookback}"
    if key not in _CALENDAR_CACHE:
        _CALENDAR_CACHE[key] = extended_trading_dates(cfg, end, lookback)
    return _CALENDAR_CACHE[key]


def trading_day_offset(cfg: MarketConfig, anchor: str, sessions_back: int) -> Optional[str]:
    """anchor — торговая дата; sessions_back=5 → пятый предыдущий торговый день."""
    dates = trading_dates_cached(cfg, date.fromisoformat(anchor))
    candidates = [d for d in dates if d <= anchor]
    if not candidates:
        return None
    anchor_day = candidates[-1]
    idx = dates.index(anchor_day) - sessions_back
    if idx < 0:
        return None
    return dates[idx]


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
            # LAST — последняя сделка; BID/OFFER — стакан; MARKETPRICE — вчера (fallback в конце в current_price_live)
            "marketdata.columns": (
                "SECID,BOARDID,LAST,BID,OFFER,MARKETPRICE,MARKETPRICETODAY,"
                "LCURRENTPRICE,WAPRICE,VALTODAY_RUR,VALTODAY,VALUE,NUMTRADES"
            ),
        }
    )
    data = fetch_json_live(f"{ISS}/engines/{cfg.engine}/markets/{cfg.market}/securities.json?{q}")
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


def _positive_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        v = float(x)
        return v if v > 0 else None
    except (TypeError, ValueError):
        return None


def current_price_live(md: Dict[str, Any]) -> Optional[float]:
    """
    Цена для колонки «текущая» на момент запроса отчёта.

    ISS: LAST — «Последняя» (цена последней сделки в сессии). Раньше MARKETPRICE шла первой, но в справочнике
    ISS для акций это «рыночная цена предыдущего дня», из‑за чего внутридневная котировка выглядела неверной.

    Если сделок ещё не было — середина спреда BID/OFFER (лучшие уровни стакана). Далее — оценки биржи.
    """
    last = _positive_float(md.get("LAST"))
    if last is not None:
        return last
    bid = _positive_float(md.get("BID"))
    offer = _positive_float(md.get("OFFER"))
    if bid is not None and offer is not None:
        return (bid + offer) / 2.0
    if bid is not None:
        return bid
    if offer is not None:
        return offer
    for key in ("LCURRENTPRICE", "MARKETPRICETODAY", "WAPRICE"):
        v = _positive_float(md.get(key))
        if v is not None:
            return v
    return _positive_float(md.get("MARKETPRICE"))


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
    h5: Optional[Dict[str, Dict[str, Any]]] = None,
    h1m: Optional[Dict[str, Dict[str, Any]]] = None,
    price_anchor: Optional[str] = None,
    md_map: Optional[Dict[str, Dict[str, Any]]] = None,
    meta: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    h0 = load_history_cached(cfg, d0) if mode == "eod" else None
    if h1 is None:
        h1 = load_history_cached(cfg, d1)
    if h2 is None:
        h2 = load_history_cached(cfg, d2)
    anchor = price_anchor or (d0 if mode == "eod" else d1)
    if h5 is None:
        d5 = trading_day_offset(cfg, anchor, 5)
        h5 = load_history_cached(cfg, d5) if d5 else {}
    if h1m is None:
        d1m = trading_day_offset(cfg, anchor, TRADING_SESSIONS_1M)
        h1m = load_history_cached(cfg, d1m) if d1m else {}

    fut_meta = load_futures_secmeta_for((h0 or h1).keys()) if cfg.key == "futures" else {}
    if cfg.key == "futures" and mode == "eod" and h0:
        prefetch_moex_futures_titles(
            [str(fut_meta.get(sid, {}).get("SHORTNAME") or h0[sid].get("SHORTNAME") or "") for sid in h0]
        )

    rows_out: List[Dict[str, Any]] = []

    if mode == "eod":
        assert h0 is not None
        for secid, r0 in h0.items():
            c0 = close_from_history_row(r0, cfg.engine)
            r1 = h1.get(secid)
            r2 = h2.get(secid)
            r5 = h5.get(secid) if h5 else None
            r1m = h1m.get(secid) if h1m else None
            c1 = close_from_history_row(r1 or {}, cfg.engine) if r1 else None
            c2 = close_from_history_row(r2 or {}, cfg.engine) if r2 else None
            c5 = close_from_history_row(r5 or {}, cfg.engine) if r5 else None
            c1m = close_from_history_row(r1m or {}, cfg.engine) if r1m else None
            if c0 is None or c1 is None or c2 is None:
                continue
            ch1 = (c0 / c1 - 1.0) * 100.0
            ch2 = (c0 / c2 - 1.0) * 100.0
            ch5 = (c0 / c5 - 1.0) * 100.0 if c5 else None
            ch1m = (c0 / c1m - 1.0) * 100.0 if c1m else None
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
                "close0": round_price_display(c0),
                "chg1": round(ch1, 1),
                "chg2": round(ch2, 1),
                "chg5": round(ch5, 1) if ch5 is not None else None,
                "chg1m": round(ch1m, 1) if ch1m is not None else None,
                "risk": round(rk * 100) if rk is not None else None,
                "ratio1": round(rp1, 1) if rp1 is not None else None,
                "ratio2": round(rp2, 1) if rp2 is not None else None,
                "vol_rub": round(vr, 0) if vr is not None else None,
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
            r5 = h5.get(secid) if h5 else None
            r1m = h1m.get(secid) if h1m else None
            c1 = close_from_history_row(r1 or {}, cfg.engine) if r1 else None
            c2 = close_from_history_row(r2 or {}, cfg.engine) if r2 else None
            c5 = close_from_history_row(r5 or {}, cfg.engine) if r5 else None
            c1m = close_from_history_row(r1m or {}, cfg.engine) if r1m else None
            if c1 is None or c2 is None:
                continue
            ch1 = (c / c1 - 1.0) * 100.0
            ch2 = (c / c2 - 1.0) * 100.0
            ch5 = (c / c5 - 1.0) * 100.0 if c5 else None
            ch1m = (c / c1m - 1.0) * 100.0 if c1m else None
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
                "close0": round_price_display(c),
                "chg1": round(ch1, 1),
                "chg2": round(ch2, 1),
                "chg5": round(ch5, 1) if ch5 is not None else None,
                "chg1m": round(ch1m, 1) if ch1m is not None else None,
                "risk": round(rk * 100) if rk is not None else None,
                "ratio1": round(rp1, 1) if rp1 is not None else None,
                "ratio2": round(rp2, 1) if rp2 is not None else None,
                "vol_rub": round(vr, 0) if vr is not None else None,
                "hl": row_highlight(rp1, rp2),
            }
            if cfg.key == "futures":
                row_l["asset_code"] = acode
            rows_out.append(row_l)

    if cfg.key == "futures" and rows_out:
        annotate_futures_maturity_rank(rows_out)

    rows_out.sort(key=lambda x: -(x.get("ratio2") or 0))
    return rows_out


def collect_eod_report_dates(stock_cfg: MarketConfig, today: date) -> List[str]:
    """Все торговые дни за последний календарный месяц (новые первыми)."""
    month_start = (today - timedelta(days=EOD_MONTH_CALENDAR_DAYS)).strftime("%Y-%m-%d")
    dates = trading_dates_cached(stock_cfg, today, EOD_MONTH_CALENDAR_DAYS + 15)
    return sorted([d for d in dates if d >= month_start], reverse=True)


def _build_market_blocks(
    key: str,
    cfg: MarketConfig,
    today: date,
    eod_candidates: List[str],
    stock_lim: Dict[str, float],
    cur_lim: Dict[str, float],
    fut_lim: Dict[str, float],
    cur_assets: List[str],
    vol_cache: Dict[str, Dict[str, float]],
) -> Tuple[str, Dict[str, Any], Dict[str, Any]]:
    d0, d1, d2 = get_three_trading_days(cfg, today)
    ref1, ref2 = live_comparison_close_dates(cfg, today)
    d5 = trading_day_offset(cfg, ref1, 5)
    d1m = trading_day_offset(cfg, ref1, TRADING_SESSIONS_1M)
    with ThreadPoolExecutor(max_workers=5) as ex:
        f_h1 = ex.submit(load_history_cached, cfg, ref1)
        f_h2 = ex.submit(load_history_cached, cfg, ref2)
        f_h5 = ex.submit(load_history_cached, cfg, d5) if d5 else None
        f_h1m = ex.submit(load_history_cached, cfg, d1m) if d1m else None
        f_md = ex.submit(live_market_block, cfg)
        h1 = f_h1.result()
        h2 = f_h2.result()
        h5 = f_h5.result() if f_h5 else {}
        h1m = f_h1m.result() if f_h1m else {}
        md_map, meta = f_md.result()

    current_block = {
        "title": cfg.title,
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
            h5=h5,
            h1m=h1m,
            price_anchor=ref1,
            md_map=md_map,
            meta=meta,
        ),
    }

    eod_by_date: Dict[str, Any] = {}

    def build_eod_day(ed0: str) -> Optional[Tuple[str, Dict[str, Any]]]:
        try:
            ed0a, ed1, ed2 = get_three_trading_days(cfg, date.fromisoformat(ed0))
        except (RuntimeError, ValueError):
            return None
        if ed0a != ed0:
            return None
        ed5 = trading_day_offset(cfg, ed0, 5)
        ed1m = trading_day_offset(cfg, ed0, TRADING_SESSIONS_1M)
        try:
            with ThreadPoolExecutor(max_workers=5) as ex:
                jobs = [
                    ex.submit(load_history_cached, cfg, ed0),
                    ex.submit(load_history_cached, cfg, ed1),
                    ex.submit(load_history_cached, cfg, ed2),
                ]
                if ed5:
                    jobs.append(ex.submit(load_history_cached, cfg, ed5))
                if ed1m:
                    jobs.append(ex.submit(load_history_cached, cfg, ed1m))
                for job in jobs:
                    job.result()
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
                price_anchor=ed0,
            )
        except Exception:
            return None
        return ed0, {
            "trading_dates": {"report": ed0, "minus1d": ed1, "minus2d": ed2},
            "rows": rows,
        }

    eod_workers = min(max(len(eod_candidates), 1), 12)
    with ThreadPoolExecutor(max_workers=eod_workers) as ex:
        results = [r for r in ex.map(build_eod_day, eod_candidates) if r is not None]
    for ed0, block in sorted(results, key=lambda x: x[0], reverse=True):
        eod_by_date[ed0] = block

    return key, current_block, {"title": cfg.title, "by_date": eod_by_date}


def generate_report() -> Path:
    t0 = time.perf_counter()
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with ThreadPoolExecutor(max_workers=4) as ex:
        f_st = ex.submit(load_stock_limits_combined)
        f_cu = ex.submit(load_risk_limits_currency)
        f_fu = ex.submit(load_futures_limits_combined)
        f_vo = ex.submit(merge_currency_volumes, load_volume_cache())
        stock_lim, stock_conc_lim = f_st.result()
        cur_lim = f_cu.result()
        fut_lim, fut_conc_lim = f_fu.result()
        vol_cache = f_vo.result()

    cur_assets = sorted(cur_lim.keys(), key=len, reverse=True)
    today = date.today()
    stock_cfg = MARKETS["stock"]
    eod_candidates = collect_eod_report_dates(stock_cfg, today)

    current_data: Dict[str, Any] = {}
    eod_data: Dict[str, Any] = {}

    with ThreadPoolExecutor(max_workers=len(MARKETS)) as ex:
        futures = [
            ex.submit(
                _build_market_blocks,
                key,
                cfg,
                today,
                eod_candidates,
                stock_lim,
                cur_lim,
                fut_lim,
                cur_assets,
                vol_cache,
            )
            for key, cfg in MARKETS.items()
        ]
        for fut in as_completed(futures):
            key, cur_block, eod_block = fut.result()
            current_data[key] = cur_block
            eod_data[key] = eod_block

    all_eod: List[str] = []
    for _m in eod_data.values():
        all_eod.extend((_m.get("by_date") or {}).keys())
    all_eod_sorted = sorted(set(all_eod), reverse=True)
    eod_max_date = all_eod_sorted[0] if all_eod_sorted else today.strftime("%Y-%m-%d")
    eod_min_date = all_eod_sorted[-1] if all_eod_sorted else ""

    conc_report_day = eod_max_date
    stock_medians, fut_medians = _load_or_build_volume_medians(conc_report_day, today)
    concentration_data = build_concentration_data(
        conc_report_day,
        stock_conc_lim,
        fut_conc_lim,
        stock_medians,
        fut_medians,
    )

    payload = {
        "generated_at": datetime.now(MSK_TZ).strftime("%Y-%m-%d %H:%M:%S"),
        "eod_max_date": eod_max_date,
        "eod_min_date": eod_min_date,
        "current": current_data,
        "eod": eod_data,
        "concentration": concentration_data,
    }

    payload_json = json.dumps(payload, ensure_ascii=False)
    (SCRIPT_DIR / "moex_report_data.json").write_text(payload_json, encoding="utf-8")
    # В HTML нельзя вставлять сырой JSON с подстрокой "</..." — закроется <script>.
    # Экранируем как \u003c в JSON — JSON.parse вернёт корректные строки.
    embed_json = payload_json.replace("</", "\\u003c/")
    html = HTML_TEMPLATE.replace("__DATA__", embed_json)
    out_path = SCRIPT_DIR / "moex_report.html"
    out_path.write_text(html, encoding="utf-8")
    elapsed = time.perf_counter() - t0
    eod_day_count = len(all_eod_sorted)
    print(f"Сборка отчёта: {elapsed:.1f} с (итогов EOD за месяц: {eod_day_count} торговых дней)")
    return out_path


HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Мониторинг достаточности ставок риска и лимитов концентрации</title>
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
    .hl-green { background: #e8f5e9 !important; }
    .hl-yellow-conc { background: #fff8e1 !important; }
    .num-null { color: #bbb; }
    th.col-emphasis, td.col-emphasis {
      background: #f3f6fb !important;
      border-left: 1px solid #d8e2f0;
    }
    th.col-emphasis { background: #e8eef8 !important; font-weight: 700; }
    tr:hover td.col-emphasis { background: #eef3fa !important; }
    tr.hl-yellow td.col-emphasis { background: #fff3b0 !important; }
    tr.hl-red td.col-emphasis { background: #ffb4b4 !important; }
    tr.hl-green td.col-emphasis { background: #dcedc8 !important; }
    tr.hl-yellow-conc td.col-emphasis { background: #ffecb3 !important; }
    table.fx-cols th:nth-child(2),
    table.fx-cols td:nth-child(2) { text-align: right; }
    tr.col-filters td { text-align: center; background: #fafafa; padding: 0.25rem 0.2rem; }
    tr.col-filters input { width: 100%; max-width: 7rem; font-size: 0.72rem; padding: 0.2rem 0.25rem; border: 1px solid var(--border); border-radius: 4px; }
    tr.col-filters input.cf-numPair { max-width: 3.1rem; display: inline-block; }
    th.col-ticker, td.col-ticker,
    th.col-vol_rub, td.col-vol_rub,
    th.col-num, td.col-num {
      white-space: nowrap;
    }
    th.col-ticker, td.col-ticker { min-width: 5.5rem; }
    th.col-vol_rub, td.col-vol_rub { min-width: 8rem; }
    th.col-shortname, td.col-shortname {
      white-space: normal;
      word-break: break-word;
      min-width: 7rem;
      max-width: 16rem;
      line-height: 1.35;
      vertical-align: top;
    }
    .export-btns { display: flex; gap: 0.5rem; flex-wrap: wrap; align-items: center; }
    .export-btns button {
      border: 1px solid var(--border); background: #fff; color: #333;
      padding: 0.35rem 0.65rem; border-radius: 6px; cursor: pointer; font-size: 0.82rem;
    }
    .export-btns button:hover { background: #f0f0f0; }
  </style>
</head>
<body>
  <header>
    <h1>Мониторинг достаточности ставок риска и лимитов концентрации</h1>
    <p class="sub"><strong>Время обновления (МСК):</strong> <span id="gen"></span></p>
  </header>
  <main>
    <div class="tabs">
      <button type="button" id="tab-cur" class="active">Текущие данные</button>
      <button type="button" id="tab-eod" class="off">Итоги торгового дня</button>
      <button type="button" id="tab-conc" class="off">Лимиты концентрации</button>
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
            <option value="all">Все фьючерсы</option>
            <option value="nearest" selected>Ближайший по сроку</option>
            <option value="two">Два ближайших по сроку</option>
          </select>
        </label>
        <span class="export-btns">
          <button type="button" id="btn-csv-cur">Скачать CSV</button>
          <button type="button" id="btn-xlsx-cur">Скачать Excel</button>
        </span>
      </div>
      <p class="meta" id="meta-cur" style="display:none;"></p>
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
        <label>Дата <select id="date-eod"></select></label>
        <label>Поиск <input type="search" id="search-eod" placeholder="Тикер или название"/></label>
        <label id="wrap-fut-eod" style="display:none;">Срочность фьючерса
          <select id="fut-mat-eod">
            <option value="all">Все фьючерсы</option>
            <option value="nearest" selected>Ближайший по сроку</option>
            <option value="two">Два ближайших по сроку</option>
          </select>
        </label>
        <span class="export-btns">
          <button type="button" id="btn-csv-eod">Скачать CSV</button>
          <button type="button" id="btn-xlsx-eod">Скачать Excel</button>
        </span>
      </div>
      <p class="meta" id="meta-eod" style="display:none;"></p>
      <div class="wrap"><table id="tbl-eod"><thead></thead><tbody></tbody></table></div>
    </section>

    <section class="panel" id="panel-conc" style="display:none;">
      <h2>Лимиты концентрации</h2>
      <div class="mkt" id="mkt-conc">
        <button type="button" data-m="stock">Фондовый</button>
        <button type="button" data-m="futures" class="off">Срочный</button>
      </div>
      <div class="toolbar">
        <label>Поиск <input type="search" id="search-conc" placeholder="Тикер или название"/></label>
        <span class="export-btns">
          <button type="button" id="btn-csv-conc">Скачать CSV</button>
          <button type="button" id="btn-xlsx-conc">Скачать Excel</button>
        </span>
      </div>
      <p class="meta" id="meta-conc" style="display:none;"></p>
      <div class="wrap"><table id="tbl-conc"><thead></thead><tbody></tbody></table></div>
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
    if (!d.concentration || typeof d.concentration !== 'object') {
      d.concentration = { report_day: '', basis: '', stock: { title: '', rows: [] }, futures: { title: '', rows: [] } };
    } else {
      if (!d.concentration.stock) d.concentration.stock = { title: '', rows: [] };
      if (!d.concentration.futures) d.concentration.futures = { title: '', rows: [] };
      if (!Array.isArray(d.concentration.stock.rows)) d.concentration.stock.rows = [];
      if (!Array.isArray(d.concentration.futures.rows)) d.concentration.futures.rows = [];
    }
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
    { key: 'chg1', label: '1д, %', num: true },
    { key: 'chg2', label: '2д, %', num: true },
    { key: 'chg5', label: '5д, %', num: true },
    { key: 'chg1m', label: '1м, %', num: true },
    { key: 'risk', label: 'СР1, %', num: true },
    { key: 'ratio1', label: '1д/СР1, %', num: true, emphasis: true },
    { key: 'ratio2', label: '2д/СР1, %', num: true, emphasis: true },
    { key: 'vol_rub', label: 'Оборот ₽', num: true },
  ];
  const COLS_CONC = [
    { key: 'ticker', label: 'Тикер', num: false },
    { key: 'shortname', label: 'Название', num: false },
    { key: 'price', label: 'Цена', num: true },
    { key: 'lk1_cur', label: 'ЛК1 текущий', num: true },
    { key: 'lk2_cur', label: 'ЛК2 текущий', num: true },
    { key: 'lk1_calc', label: 'ЛК1 расчётный', num: true },
    { key: 'lk2_calc', label: 'ЛК2 расчётный', num: true },
    { key: 'lk2_cur_rub', label: 'ЛК2 текущий (руб.)', num: true, emphasis: true },
    { key: 'lk2_calc_rub', label: 'ЛК2 расчётный (руб.)', num: true, emphasis: true },
    { key: 'lk2_delta_pct', label: 'Относительная дельта', num: true, emphasis: true },
  ];
  function colsForConc(market) {
    return COLS_CONC.map(c => {
      if (c.key === 'price' && market === 'futures') {
        return Object.assign({}, c, { label: 'Цена БА' });
      }
      if (c.key === 'shortname' && market === 'futures') {
        return Object.assign({}, c, { label: 'Фьючерсный контракт на' });
      }
      return c;
    });
  }
  function addThousandsSpaces(s) {
    const parts = String(s).split('.');
    parts[0] = parts[0].replace(/\\B(?=(\\d{3})+(?!\\d))/g, ' ');
    return parts.join('.');
  }
  function roundPriceSmart(n) {
    const v = Number(n);
    if (isNaN(v)) return null;
    const av = Math.abs(v);
    if (av === 0) return 0;
    if (av >= 0.1) return Math.round(v * 100) / 100;
    let pos = 0;
    for (let i = 1; i <= 14; i++) {
      if (av * Math.pow(10, i) >= 1) { pos = i; break; }
    }
    if (!pos) pos = 14;
    const mult = Math.pow(10, pos + 1);
    return Math.round(v * mult) / mult;
  }
  function fmtPriceDot(x) {
    if (x == null || x === '') return null;
    const n = roundPriceSmart(x);
    if (n == null || isNaN(n)) return null;
    return addThousandsSpaces(String(parseFloat(n.toFixed(10))));
  }
  function fmtNumDot(x, decimals) {
    if (x == null || x === '') return null;
    const n = Number(x);
    if (isNaN(n)) return null;
    return addThousandsSpaces(n.toFixed(decimals == null ? 1 : decimals));
  }
  function fmtIntDot(x) {
    if (x == null || x === '') return null;
    const n = Number(x);
    if (isNaN(n)) return null;
    return addThousandsSpaces(String(Math.round(n)));
  }
  function colsForMarket(eod, market) {
    const pl = eod ? 'Цена закрытия' : 'Цена';
    let base = COLS;
    if (market === 'currency') {
      base = COLS.filter(c => c.key !== 'shortname' && c.key !== 'vol_rub');
    }
    return base.map(c => (c.key === 'close0' ? Object.assign({}, c, { label: pl }) : c));
  }
  let mode = 'cur';
  let marketCur = 'stock';
  let marketEod = 'stock';
  let marketConc = 'stock';
  let sortCol = 'ratio2';
  let sortDir = -1;
  let sortColE = 'ratio2';
  let sortDirE = -1;
  let sortColC = 'lk2_delta_pct';
  let sortDirC = -1;
  const filtersCurByMkt = { stock: {}, currency: {}, futures: {} };
  const filtersEodByMkt = { stock: {}, currency: {}, futures: {} };
  const filtersConcByMkt = { stock: {}, futures: {} };

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
  function getRowsConc() {
    const block = DATA.concentration && DATA.concentration[marketConc];
    return (block && block.rows) ? block.rows.slice() : [];
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
  function colClass(c) {
    let cl = 'col-' + c.key;
    if (c.num) cl += ' col-num';
    if (c.emphasis) cl += ' col-emphasis';
    return cl;
  }
  function renderThead(tblId, sortKey, sortD, onHeadClick, colList, filt, onFilterTyping) {
    const thead = document.querySelector('#' + tblId + ' thead');
    const cols = colList || COLS;
    cols.forEach(c => ensureSlot(filt, c));
    const row1 = '<tr>' + cols.map(c => {
      const cl = (c.key === sortKey ? 'sorted' + (sortD < 0 ? ' desc' : '') : '') + ' ' + colClass(c);
      return '<th data-k="' + c.key + '" class="' + cl.trim() + '">' + c.label + '</th>';
    }).join('') + '</tr>';
    const row2 = '<tr class="col-filters">' + cols.map(c => {
      const s = filt[c.key];
      const tdCl = colClass(c);
      if (c.num) {
        const mn = (s && s.min) != null ? String(s.min) : '';
        const mx = (s && s.max) != null ? String(s.max) : '';
        return '<td class="' + tdCl + '" onclick="event.stopPropagation()"><input type="number" step="any" class="cf-numPair" placeholder="≥" data-k="' + c.key + '" data-part="min" value="' + escapeAttr(mn) + '"/> ' +
          '<input type="number" step="any" class="cf-numPair" placeholder="≤" data-k="' + c.key + '" data-part="max" value="' + escapeAttr(mx) + '"/></td>';
      }
      const tv = (s && s.text) != null ? String(s.text) : '';
      return '<td class="' + tdCl + '" onclick="event.stopPropagation()"><input type="text" class="cf-text" placeholder="содержит…" data-k="' + c.key + '" value="' + escapeAttr(tv) + '"/></td>';
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
    if (keys.indexOf(sortCol) < 0) { sortCol = 'ratio2'; sortDir = -1; }
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
    if (keysE.indexOf(sortColE) < 0) { sortColE = 'ratio2'; sortDirE = -1; }
    rows = sortRows(rows, sortColE, sortDirE, ecols);
    return { rows, ecols };
  }
  function computeRowsConc() {
    let rows = getRowsConc();
    rows = filterRows(rows, document.getElementById('search-conc').value);
    const ccols = colsForConc(marketConc);
    const filt = filtersConcByMkt[marketConc];
    ccols.forEach(c => ensureSlot(filt, c));
    rows = applyColFilters(rows, ccols, filt);
    const keysC = ccols.map(c => c.key);
    if (keysC.indexOf(sortColC) < 0) { sortColC = 'lk2_delta_pct'; sortDirC = -1; }
    rows = sortRows(rows, sortColC, sortDirC, ccols);
    return { rows, ccols };
  }
  function cellHtml(c, r, f) {
    const k = c.key;
    const cl = colClass(c);
    if (k === 'ticker') {
      const tick = (r.ticker != null && r.ticker !== '') ? r.ticker : r.secid;
      return '<td class="' + cl + '"><strong>' + escapeHtml(String(tick)) + '</strong></td>';
    }
    if (k === 'shortname') return '<td class="' + cl + '">' + escapeHtml(String(r.shortname || '')) + '</td>';
    if (k === 'price' || k === 'close0') {
      const v = fmtPriceDot(r[k]);
      return '<td class="' + cl + '">' + (v != null ? v : f(r[k])) + '</td>';
    }
    if (k === 'lk2_delta_pct') {
      const v = r[k];
      if (v == null || v === '') return '<td class="' + cl + '">' + f(v) + '</td>';
      const n = Number(v);
      const s = (n > 0 ? '+' : '') + fmtNumDot(n, 1) + '%';
      return '<td class="' + cl + '">' + s + '</td>';
    }
    if (k === 'lk1_cur' || k === 'lk2_cur' || k === 'lk1_calc' || k === 'lk2_calc' || k === 'lk2_cur_rub' || k === 'lk2_calc_rub' || k === 'vol_rub') {
      const v = fmtIntDot(r[k]);
      return '<td class="' + cl + '">' + (v != null ? v : f(r[k])) + '</td>';
    }
    if (k === 'risk') {
      const v = fmtIntDot(r[k]);
      return '<td class="' + cl + '">' + (v != null ? v : f(r[k])) + '</td>';
    }
    if (k === 'chg1' || k === 'chg2' || k === 'chg5' || k === 'chg1m' || k === 'ratio1' || k === 'ratio2') {
      const v = fmtNumDot(r[k], 1);
      return '<td class="' + cl + '">' + (v != null ? v : f(r[k])) + '</td>';
    }
    return '<td class="' + cl + '"></td>';
  }
  function renderTbody(tbody, rows, colList) {
    const cols = colList || COLS;
    tbody.innerHTML = rows.map(r => {
      const f = (x) => (x != null && x !== '') ? x : '<span class="num-null">—</span>';
      return '<tr class="' + (r.hl || '') + '">' + cols.map(c => cellHtml(c, r, f)).join('') + '</tr>';
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
  function refreshConcBodyOnly() {
    const { rows, ccols } = computeRowsConc();
    renderTbody(document.querySelector('#tbl-conc tbody'), rows, ccols);
  }
  function refreshCur() {
    if (!DATA || !DATA.current) return;
    const inp = document.getElementById('search-cur');
    inp.placeholder = (marketCur === 'currency') ? 'Тикер' : 'Тикер или название';
    document.getElementById('wrap-fut-cur').style.display = (marketCur === 'futures') ? '' : 'none';
    const { rows, ccols } = computeRowsCur();
    document.getElementById('tbl-cur').className = (marketCur === 'currency') ? 'fx-cols' : '';
    const filt = filtersCurByMkt[marketCur];
    renderThead('tbl-cur', sortCol, sortDir, (k) => {
      if (k === sortCol) sortDir = -sortDir; else { sortCol = k; sortDir = -1; }
      refreshCur();
    }, ccols, filt, refreshCurBodyOnly);
    renderTbody(document.querySelector('#tbl-cur tbody'), rows, ccols);
  }
  function eodDatesForMarket() {
    const block = DATA.eod[marketEod];
    const bd = block && block.by_date ? block.by_date : {};
    return Object.keys(bd).sort().reverse();
  }
  function populateEodDateSelect(preserve) {
    const sel = document.getElementById('date-eod');
    const prev = preserve ? sel.value : '';
    const opts = eodDatesForMarket();
    sel.innerHTML = opts.map(function(d) {
      return '<option value="' + escapeAttr(d) + '">' + escapeAttr(d) + '</option>';
    }).join('');
    if (opts.length) {
      if (prev && opts.indexOf(prev) >= 0) sel.value = prev;
      else sel.value = opts[0];
    }
  }
  function refreshEod() {
    if (!DATA || !DATA.eod) return;
    populateEodDateSelect(true);
    const inpe = document.getElementById('search-eod');
    inpe.placeholder = (marketEod === 'currency') ? 'Тикер' : 'Тикер или название';
    document.getElementById('wrap-fut-eod').style.display = (marketEod === 'futures') ? '' : 'none';
    const { rows, ecols } = computeRowsEod();
    document.getElementById('tbl-eod').className = (marketEod === 'currency') ? 'fx-cols' : '';
    const filt = filtersEodByMkt[marketEod];
    renderThead('tbl-eod', sortColE, sortDirE, (k) => {
      if (k === sortColE) sortDirE = -sortDirE; else { sortColE = k; sortDirE = -1; }
      refreshEod();
    }, ecols, filt, refreshEodBodyOnly);
    renderTbody(document.querySelector('#tbl-eod tbody'), rows, ecols);
  }
  function refreshConc() {
    if (!DATA || !DATA.concentration) return;
    const { rows, ccols } = computeRowsConc();
    const filt = filtersConcByMkt[marketConc];
    renderThead('tbl-conc', sortColC, sortDirC, (k) => {
      if (k === sortColC) sortDirC = -sortDirC; else { sortColC = k; sortDirC = -1; }
      refreshConc();
    }, ccols, filt, refreshConcBodyOnly);
    renderTbody(document.querySelector('#tbl-conc tbody'), rows, ccols);
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
  function showPanel(name) {
    ['cur', 'eod', 'conc'].forEach(p => {
      document.getElementById('panel-' + p).style.display = (p === name) ? 'block' : 'none';
      document.getElementById('tab-' + p).className = (p === name) ? 'active' : 'off';
    });
    mode = name;
  }
  document.getElementById('tab-cur').onclick = () => showPanel('cur');
  document.getElementById('tab-eod').onclick = () => showPanel('eod');
  document.getElementById('tab-conc').onclick = () => { showPanel('conc'); refreshConc(); };
  function bindMkt(id, isEod, isConc) {
    document.querySelectorAll('#' + id + ' button').forEach(btn => {
      btn.onclick = () => {
        document.querySelectorAll('#' + id + ' button').forEach(b => { b.className = 'off'; });
        btn.className = '';
        if (isEod) { marketEod = btn.dataset.m; refreshEod(); }
        else if (isConc) { marketConc = btn.dataset.m; refreshConc(); }
        else { marketCur = btn.dataset.m; refreshCur(); }
      };
    });
  }
  bindMkt('mkt-cur', false, false);
  bindMkt('mkt-eod', true, false);
  bindMkt('mkt-conc', false, true);
  document.getElementById('search-cur').oninput = refreshCurBodyOnly;
  document.getElementById('search-eod').oninput = refreshEodBodyOnly;
  document.getElementById('search-conc').oninput = refreshConcBodyOnly;
  document.getElementById('fut-mat-cur').onchange = refreshCur;
  document.getElementById('fut-mat-eod').onchange = refreshEod;

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
      refreshConc();
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
  document.getElementById('btn-csv-conc').onclick = () => {
    const { rows, ccols } = computeRowsConc();
    const rd = (DATA.concentration && DATA.concentration.report_day) || '';
    exportCsv('conc_' + marketConc + '_' + rd + '_' + stamp() + '.csv', rows, ccols);
  };
  document.getElementById('btn-xlsx-conc').onclick = () => {
    const { rows, ccols } = computeRowsConc();
    const rd = (DATA.concentration && DATA.concentration.report_day) || '';
    exportXlsx('conc_' + marketConc + '_' + rd + '_' + stamp() + '.xlsx', rows, ccols);
  };

  async function startApp() {
    document.getElementById('gen').textContent = canPoll ? 'Загрузка…' : (DATA.generated_at || '—');
    await loadInitialData();
    updateGenLabel();
    document.getElementById('date-eod').onchange = refreshEod;
    refreshCur();
    refreshEod();
    refreshConc();
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
