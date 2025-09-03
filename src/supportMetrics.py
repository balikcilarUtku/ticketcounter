from __future__ import annotations
from html import parser
from pathlib import Path
from dataclasses import dataclass
from typing import Optional, List
import argparse
import sys
import re
import pandas as pd
from dateutil import parser as dtparser

COLUMN_MAP_OVERRIDE = {

    "Şirket Adı": "company",
    "Konu": "subject",
    "Durum": "status",
    "Aciliyet Durumu": "priority",
    "Atanan Destek Personeli": "assignee",
    "Oluşturma Tarihi": "created_at",
    "Güncelleyen": "actor",
    "Güncelleme Tarihi": "status_changed_at",
    "Kapatan Kullanıcı": "closed_by",
    "Kaynak": "source",
}

SYNONYMS = {
    "ticket_id": {
        "ticket_id","ticket id","id","talep id","talep no","bilet id","request id","issue id","key","issue key"
    },
    "status": {
        "status","durum","state","ticket status"
    },
    "status_changed_at": {
        "status_changed_at","status changed at","status change time","updated_at","update time","updated",
        "transition time","event time","durum tarihi","durum tarih","durum zamanı","son güncelleme","son guncelleme"
    },
    "actor": {
        "actor","user","updated_by","changer","changed by","işlem yapan","islem yapan","değiştiren",
        "degistiren","transitioner"
    },
    "assignee": {
        "assignee","atanan","atanan kisi","owner","sahibi","responsible"
    },
    "closed_by": {
        "closed_by","closed by","kapatan","resolver","resolved_by","resolved by","cozen","çözen"
    },
}

def _norm(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip().lower()
    tr = str.maketrans("çğıöşüı", "cgiosui")
    s = s.translate(tr)

    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def apply_column_mapping(df: pd.DataFrame) -> pd.DataFrame:

    if COLUMN_MAP_OVERRIDE:
        df = df.rename(columns=COLUMN_MAP_OVERRIDE)

    current_cols = set(df.columns)
    wanted = ["ticket_id", "status", "status_changed_at", "actor", "assignee", "closed_by"]

    norm_to_orig = {}
    for c in df.columns:
        n = _norm(c)
        if n and n not in norm_to_orig:
            norm_to_orig[n] = c

    auto_map = {}
    for target in wanted:
        if target in current_cols:
            continue
        for syn in SYNONYMS.get(target, []):
            n = _norm(syn)
            if n in norm_to_orig:
                auto_map[norm_to_orig[n]] = target
                break

    if auto_map:
        df = df.rename(columns=auto_map)

    for col in wanted:
        if col not in df.columns:
            df[col] = ""

    applied = {}
    applied.update(COLUMN_MAP_OVERRIDE)
    applied.update(auto_map)
    if applied:
        print("Uygulanan kolon eşlemeleri:", applied)

    return df



CLOSED_STATUSES = {"closed", "resolved", "done", "completed"}

EXPECTED_COLUMNS = [
    "ticket_id",
    "status",              
    "status_changed_at",   
    "actor",               

    "assignee",
    "closed_by",           
]


@dataclass
class TicketCloseInfo:
    ticket_id: str
    closed_by_final: str
    closed_at: Optional[pd.Timestamp]

def read_table(path: Path, sheet_name: Optional[str] = None) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Girdi dosyası yok: {path}")

    suf = path.suffix.lower()

    if suf == ".xlsx":
        if sheet_name:
            try:
                df = pd.read_excel(path, dtype=str, sheet_name=sheet_name, engine="openpyxl")
            except ValueError:
                with pd.ExcelFile(path, engine="openpyxl") as x:
                    first = x.sheet_names[0]
                print(f"UYARI: '{sheet_name}' bulunamadı, '{first}' sayfası okunuyor.", file=sys.stderr)
                df = pd.read_excel(path, dtype=str, sheet_name=first, engine="openpyxl")
        else:
            df = pd.read_excel(path, dtype=str, engine="openpyxl")

    elif suf == ".xls":
        if sheet_name:
            try:
                df = pd.read_excel(path, dtype=str, sheet_name=sheet_name, engine="xlrd")
            except ValueError:
                with pd.ExcelFile(path, engine="xlrd") as x:
                    first = x.sheet_names[0]
                print(f"UYARI: '{sheet_name}' bulunamadı, '{first}' sayfası okunuyor.", file=sys.stderr)
                df = pd.read_excel(path, dtype=str, sheet_name=first, engine="xlrd")
        else:
            df = pd.read_excel(path, dtype=str, engine="xlrd")

    elif suf == ".csv":
        df = pd.read_csv(path, dtype=str)

    elif suf in {".json", ".ndjson"}:
        try:
            df = pd.read_json(path, lines=True, dtype=str)
        except ValueError:
            df = pd.read_json(path, dtype=str)

    else:
        raise ValueError("Desteklenen formatlar: CSV/JSON/XLS/XLSX")



    df = df.fillna("")
    df = apply_column_mapping(df)
    missing = [c for c in ["ticket_id", "status", "status_changed_at"] if c not in df.columns]
    if missing:
        print(f"UYARI: Beklenen kolonlar yok: {missing}. Mevcut kolonlar: {list(df.columns)}", file=sys.stderr)

    if "status" in df.columns:
        df["status_norm"] = df["status"].astype(str).str.lower().str.strip()
    else:
        df["status_norm"] = ""

    if "status_changed_at" in df.columns:
        def _parse(x: str):
            x = str(x).strip()
            if not x:
                return pd.NaT
            try:
                return pd.to_datetime(dtparser.parse(x))
            except Exception:
                return pd.NaT
        df["status_changed_at"] = df["status_changed_at"].apply(_parse)

    for col in ["ticket_id", "actor", "assignee", "closed_by"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
        else:
            df[col] = ""

    return df


    df = df.fillna("")

    missing = [c for c in ["ticket_id", "status", "status_changed_at"] if c not in df.columns]
    if missing:
        print(f"UYARI: Beklenen kolonlar yok: {missing}. Mevcut kolonlar: {list(df.columns)}", file=sys.stderr)

    if "status" in df.columns:
        df["status_norm"] = df["status"].astype(str).str.lower().str.strip()
    else:
        df["status_norm"] = ""

    if "status_changed_at" in df.columns:
        def _parse(x: str):
            x = str(x).strip()
            if not x:
                return pd.NaT
            try:
                return pd.to_datetime(dtparser.parse(x))
            except Exception:
                return pd.NaT
        df["status_changed_at"] = df["status_changed_at"].apply(_parse)

    for col in ["ticket_id", "actor", "assignee", "closed_by"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    return df


def infer_closed_by(df: pd.DataFrame) -> pd.DataFrame:
    df["is_closed_status"] = df["status_norm"].isin(CLOSED_STATUSES)

    def _resolve_one(group: pd.DataFrame) -> str:
        explicit = group[group.get("closed_by", "").astype(str).str.len() > 0]
        if not explicit.empty:
            if "status_changed_at" in explicit.columns and explicit["status_changed_at"].notna().any():
                idx = explicit["status_changed_at"].idxmax()
                return str(explicit.loc[idx, "closed_by"]).strip()
            return str(explicit.iloc[-1]["closed_by"]).strip()

        closed_rows = group[group["is_closed_status"] == True]
        if closed_rows.empty:
            return "" 
        if "status_changed_at" in closed_rows.columns and closed_rows["status_changed_at"].notna().any():
            idx = closed_rows["status_changed_at"].idxmax()
            return str(closed_rows.loc[idx, "actor"]).strip() if "actor" in closed_rows.columns else ""
        return str(closed_rows.iloc[-1].get("actor", "")).strip()

    resolved = (
        df.groupby("ticket_id", dropna=False, as_index=False)
          .apply(_resolve_one)
          .rename(columns={None: "closed_by_final"})
    )

    if "ticket_id" not in resolved.columns:
        resolved = resolved.reset_index().rename(columns={"level_0": "ticket_id", 0: "closed_by_final"})
    else:
        resolved = resolved.rename(columns={0: "closed_by_final"})

    return resolved[["ticket_id", "closed_by_final"]]


def compute_closed_at(df: pd.DataFrame) -> pd.DataFrame:
    closed_rows = df[df["status_norm"].isin(CLOSED_STATUSES)].copy()
    if closed_rows.empty:
        return pd.DataFrame(columns=["ticket_id", "closed_at"])  

    last_closed = (
        closed_rows.sort_values(["ticket_id", "status_changed_at"], na_position="last")
                   .drop_duplicates(subset=["ticket_id"], keep="last")
                   [["ticket_id", "status_changed_at"]]
                   .rename(columns={"status_changed_at": "closed_at"})
    )
    return last_closed


def aggregate_reports(resolved: pd.DataFrame, closed_at: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    ticket_final = resolved.merge(closed_at, on="ticket_id", how="left")

    summary = (
        ticket_final[ticket_final["closed_by_final"].str.len() > 0]
        .groupby("closed_by_final", as_index=False)
        .agg(total_closed=("ticket_id", "count"))
        .sort_values("total_closed", ascending=False)
    )

    if "closed_at" in ticket_final.columns and ticket_final["closed_at"].notna().any():
        ticket_final["year_month"] = ticket_final["closed_at"].dt.to_period("M").astype(str)
        by_month = (
            ticket_final[ticket_final["closed_by_final"].str.len() > 0]
            .groupby(["closed_by_final", "year_month"], as_index=False)
            .agg(closed_count=("ticket_id", "count"))
            .sort_values(["closed_by_final", "year_month"])
        )
    else:
        by_month = pd.DataFrame(columns=["closed_by_final", "year_month", "closed_count"])

    return summary, by_month

def custom_flat_reports(df_raw: pd.DataFrame, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    df = df_raw.copy()
    if "created_at" in df.columns:
        df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")

    cols_view = {
        "subject": "Konu",
        "priority": "Aciliyet Durumu",
        "closed_by": "Kapatan Kullanıcı",
        "company": "Şirket Adı",
        "created_at": "Oluşturma Tarihi",
        "assignee": "Atanan Kullanıcı",
    }
    selected = [c for c in cols_view.keys() if c in df.columns]
    view_df = df[selected].rename(columns={k: v for k, v in cols_view.items() if k in selected})
    view_df.to_csv(out_dir / "flat_selected_columns.csv", index=False)

    def _count(col: str, out_name: str):
        if col not in df.columns:
            return
        tmp = (
            df[df[col].astype(str).str.len() > 0]
            .groupby(col, as_index=False)
            .size()
            .rename(columns={col: out_name, "size": "adet"})
            .sort_values("adet", ascending=False)
        )
        tmp.to_csv(out_dir / f"count_by_{col}.csv", index=False)

    _count("closed_by", "Kapatan Kullanıcı")
    _count("priority", "Aciliyet Durumu")
    _count("assignee", "Atanan Kullanıcı")

def run_pipeline(input_path: Path, out_dir: Path, sheet_name: Optional[str] = None,
                 start_date: Optional[str] = None, end_date: Optional[str] = None) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    df = read_table(input_path, sheet_name=sheet_name)
    resolved = infer_closed_by(df)
    closed_at = compute_closed_at(df)
    if start_date or end_date:
        if "closed_at" in closed_at.columns:
            mask = pd.Series([True] * len(closed_at))
            if start_date:
                mask &= closed_at["closed_at"] >= pd.to_datetime(start_date)
            if end_date:
                mask &= closed_at["closed_at"] <= pd.to_datetime(end_date)
            closed_at = closed_at[mask]

    summary, by_month = aggregate_reports(resolved, closed_at)


    summary_path = out_dir / "closed_by_summary.csv"
    summary.to_csv(summary_path, index=False)

    print("\n✅ Bitti. Üretilen dosya:")
    print(f"  - {summary_path}\n")

def count_closed_by_in_range(input_path: Path, out_dir: Path,
                             sheet_name: Optional[str],
                             start_date: Optional[str],
                             end_date: Optional[str]) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    df = read_table(input_path, sheet_name=sheet_name)

    # Tarih kolonu: öncelik status_changed_at; yoksa created_at
    date_col = "status_changed_at" if "status_changed_at" in df.columns else "created_at"
    if date_col not in df.columns:
        raise ValueError("Tarih kolonu bulunamadı (status_changed_at / created_at).")

    if start_date or end_date:
        start_ts = pd.to_datetime(start_date) if start_date else None
        end_ts = pd.to_datetime(end_date) if end_date else None
        m = pd.Series([True] * len(df))
        if start_ts is not None:
            m &= pd.to_datetime(df[date_col], errors="coerce") >= start_ts
        if end_ts is not None:
            m &= pd.to_datetime(df[date_col], errors="coerce") <= end_ts
        df = df[m]

    if "assignee" not in df.columns:
        raise ValueError("Atanan Kullanıcı (assignee) kolonu bulunamadı.")
    df = df[df["assignee"].astype(str).str.strip().str.len() > 0]

    summary = (
        df.groupby("assignee", as_index=False)
          .size()
          # UYUM için kolon adları aynı kalıyor:
          .rename(columns={"assignee": "closed_by_final", "size": "total_closed"})
          .sort_values("total_closed", ascending=False)
    )

    out_path = out_dir / "closed_by_summary.csv"  # dosya adı da aynı kalıyor
    summary.to_csv(out_path, index=False)
    print("\nBitti. Üretilen dosya:")
    print(f"  - {out_path}\n")




def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Support Metrics — Ticket kapatan kullanıcı raporu")
    parser.add_argument("--in", dest="inp", required=False, help="Girdi dosyası (CSV/JSON/XLS/XLSX)")
    parser.add_argument("--out", dest="out", required=False, default="outputs/", help="Çıktı klasörü")
    parser.add_argument("--sheet", dest="sheet", required=False, help="Excel sayfa adı (opsiyonel)")
    parser.add_argument("--make-sample", action="store_true", help="data/tickets_sample.csv üret ve onunla çalış")
    parser.add_argument("--flat", action="store_true", help="Düz tablo modu (custom_flat_reports çalıştır)")
    parser.add_argument("--start", dest="start", help="Başlangıç tarihi (YYYY-MM-DD)")
    parser.add_argument("--end", dest="end", help="Bitiş tarihi (YYYY-MM-DD)")


    return parser.parse_args(argv)



def main(argv: Optional[List[str]] = None) -> None:
    args = parse_args(argv)

    out_dir = Path(args.out)

    if args.make_sample:
        sample_path = Path("data/tickets_sample.csv")
        sample_path.parent.mkdir(parents=True, exist_ok=True)
        # make_sample_csv(sample_path)  # artık yok
        print(f"Örnek veri yolu hazır: {sample_path}")
        run_pipeline(sample_path, out_dir)
        return


    if not args.inp:
        print("Hata: --in parametresi gerekli veya --make-sample kullanın.", file=sys.stderr)
        sys.exit(2)

    input_path = Path(args.inp)
    count_closed_by_in_range(input_path, out_dir, args.sheet, args.start, args.end)
    return


if __name__ == "__main__":
    main()
