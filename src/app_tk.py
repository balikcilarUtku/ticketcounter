from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import customtkinter as ctk
import matplotlib
import pandas as pd
import requests
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from tkcalendar import DateEntry
from tkinter import filedialog, messagebox, ttk

matplotlib.use("TkAgg")
import matplotlib.pyplot as plt


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

APP_BG = "#f7efe5"
CARD_BG = "#fffaf5"
CARD_ALT = "#f1dfcd"
TEXT_MAIN = "#4b3425"
TEXT_MUTED = "#8b6b57"
ACCENT = "#c7794b"
ACCENT_HOVER = "#b86a3f"
ACCENT_2 = "#9f6c50"
ACCENT_2_HOVER = "#8c5c42"
WARN = "#d59b4c"
TABLE_BG = "#f9f2ea"
TABLE_ALT = "#efe2d3"
BORDER = "#dcc6b2"
CHART_BG = "#fff8f0"


@dataclass
class SourcePayload:
    df: pd.DataFrame
    source_label: str


class DataSourceError(RuntimeError):
    pass


class TicketDataService:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "TicketCounter/2.0"})

    def load_from_file(self, path: Path, sheet_name: Optional[str] = None) -> SourcePayload:
        df = self._read_table(path, sheet_name)
        return SourcePayload(df=df, source_label=f"Excel / CSV • {path.name}")

    def load_from_api(self, url: str, token: str = "", timeout: int = 30) -> SourcePayload:
        if not url.strip():
            raise DataSourceError("API adresi boş olamaz.")

        headers = {}
        if token.strip():
            headers["Authorization"] = f"Bearer {token.strip()}"

        response = self.session.get(url.strip(), headers=headers, timeout=timeout)
        response.raise_for_status()

        payload = response.json()
        if isinstance(payload, dict):
            for key in ("data", "results", "items", "tickets", "records"):
                if isinstance(payload.get(key), list):
                    payload = payload[key]
                    break
            else:
                payload = [payload]

        if not isinstance(payload, list):
            raise DataSourceError("API çıktısı liste formatında değil. Beklenen: liste veya data/results/items/tickets alanı.")

        df = pd.DataFrame(payload)
        if df.empty:
            raise DataSourceError("API veri döndürdü ama kayıt bulunamadı.")

        df = self._clean_df(df)
        return SourcePayload(df=df, source_label="API bağlantısı")

    def summarize_assignees(self, df: pd.DataFrame, start: str | None, end: str | None) -> pd.DataFrame:
        df = self._apply_mapping(df.copy())
        date_col, date_series = self._pick_best_date_series(df)

        if start or end:
            if date_col is None:
                raise DataSourceError("Tarih filtresi istendi ama kullanılabilir tarih kolonu bulunamadı.")
            mask = pd.Series(True, index=df.index)
            if start:
                start_ts = pd.to_datetime(start).floor("D")
                mask &= date_series >= start_ts
            if end:
                end_ts = pd.to_datetime(end).floor("D") + pd.Timedelta(days=1)
                mask &= date_series < end_ts
            df = df[mask]

        df = df[df["assignee"].astype(str).str.strip().str.len() > 0]

        summary = (
            df.groupby("assignee", as_index=False)
            .size()
            .rename(columns={"assignee": "Kullanıcı", "size": "Ticket Adedi"})
            .sort_values("Ticket Adedi", ascending=False)
        )
        return summary

    def compute_stats(self, summary: pd.DataFrame) -> dict[str, int | str]:
        if summary.empty:
            return {"total": 0, "people": 0, "top_name": "—", "top_count": 0}

        total = int(pd.to_numeric(summary["Ticket Adedi"], errors="coerce").fillna(0).sum())
        top_row = summary.iloc[0]
        return {
            "total": total,
            "people": int(len(summary)),
            "top_name": str(top_row["Kullanıcı"]),
            "top_count": int(top_row["Ticket Adedi"]),
        }

    def apply_budget_distribution(self, summary: pd.DataFrame, budget: float) -> pd.DataFrame:
        if summary.empty:
            raise DataSourceError("Önce analiz sonucu oluşmalı.")
        if budget < 0:
            raise DataSourceError("Bütçe negatif olamaz.")

        result = summary.copy()
        counts = pd.to_numeric(result["Ticket Adedi"], errors="coerce").fillna(0).astype(float)
        total = float(counts.sum())

        if total <= 0:
            result["Yüzde"] = 0.0
            result["Hakediş (TL)"] = 0.0
            return result

        result["Yüzde"] = ((counts / total) * 100.0).round(2)
        result["Hakediş (TL)"] = ((counts / total) * float(budget)).round(2)
        return result

    def _read_table(self, path: Path, sheet_name: str | None) -> pd.DataFrame:
        if not path.exists():
            raise DataSourceError(f"Dosya bulunamadı: {path}")

        suffix = path.suffix.lower()
        if suffix == ".xlsx":
            df = pd.read_excel(path, dtype=str, sheet_name=sheet_name or 0, engine="openpyxl")
            return self._clean_df(df)

        if suffix == ".xls":
            try:
                df = pd.read_excel(path, dtype=str, sheet_name=sheet_name or 0, engine="xlrd")
                return self._clean_df(df)
            except Exception:
                pass

            try:
                with open(path, "rb") as f:
                    head = f.read(2048)
                head_text = head.decode("utf-8", errors="ignore").lower()
                if "<?xml" in head_text and (
                    "spreadsheet" in head_text or "urn:schemas-microsoft-com:office:spreadsheet" in head_text
                ):
                    from lxml import etree

                    ns = {"ss": "urn:schemas-microsoft-com:office:spreadsheet"}
                    tree = etree.parse(str(path))
                    rows_xml = tree.xpath("//ss:Worksheet[1]//ss:Table//ss:Row", namespaces=ns)
                    rows = []
                    max_len = 0
                    for row in rows_xml:
                        vals = [(d.text or "") for d in row.xpath("./ss:Cell/ss:Data", namespaces=ns)]
                        rows.append(vals)
                        max_len = max(max_len, len(vals))
                    if not rows:
                        raise DataSourceError("XML Spreadsheet içeriği boş görünüyor.")
                    rows = [r + [""] * (max_len - len(r)) for r in rows]
                    df = pd.DataFrame(rows[1:], columns=[str(h or "").strip() for h in rows[0]])
                    return self._clean_df(df)
            except Exception:
                pass

            for enc in ("utf-8-sig", "cp1254", "latin1"):
                for sep in ("|", ";", ",", "\t", None):
                    try:
                        df = pd.read_csv(path, dtype=str, sep=sep, encoding=enc, engine="python")
                        return self._clean_df(df)
                    except Exception:
                        continue
            raise DataSourceError(".xls dosyası okunamadı.")

        if suffix == ".csv":
            df = pd.read_csv(path, dtype=str)
            return self._clean_df(df)

        raise DataSourceError("Lütfen XLSX / XLS / CSV formatında dosya seç.")

    def _clean_df(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df.columns = df.columns.astype(str).str.replace("\ufeff", "", regex=False).str.strip()
        for col in df.columns:
            df[col] = df[col].astype(str).str.replace("\ufeff", "", regex=False).str.strip()
        return df.fillna("")

    def _apply_mapping(self, df: pd.DataFrame) -> pd.DataFrame:
        if COLUMN_MAP_OVERRIDE:
            df = df.rename(columns=COLUMN_MAP_OVERRIDE)

        for col in ["closed_by", "status_changed_at", "created_at", "assignee"]:
            if col not in df.columns:
                df[col] = ""

        def parse_assignee(val: str) -> str:
            if not val:
                return ""
            s = str(val).strip()
            if s.startswith("{") and s.endswith("}"):
                try:
                    obj = json.loads(s)
                    if "adi_soyadi" in obj and str(obj["adi_soyadi"]).strip():
                        return str(obj["adi_soyadi"]).strip()
                except Exception:
                    return s
            return s

        df["assignee"] = df["assignee"].apply(parse_assignee).astype(str).str.strip()
        return df

    def _pick_best_date_series(self, df: pd.DataFrame):
        candidates = []
        if "status_changed_at" in df.columns:
            candidates.append("status_changed_at")
        if "created_at" in df.columns:
            candidates.append("created_at")

        best_col, best_series, best_ok = None, None, -1
        for col in candidates:
            raw = df[col]
            series = pd.to_datetime(raw, errors="coerce")
            ok = int(series.notna().sum())
            if ok < max(1, int(len(raw) * 0.2)):
                nums = pd.to_numeric(raw, errors="coerce")
                alt = pd.to_datetime(nums, unit="D", origin="1899-12-30", errors="coerce")
                alt_ok = int(alt.notna().sum())
                if alt_ok > ok:
                    series, ok = alt, alt_ok
            if ok > best_ok:
                best_col, best_series, best_ok = col, series, ok

        return best_col, best_series


class App(ctk.CTk):
    def __init__(self) -> None:
        super().__init__()
        ctk.set_appearance_mode("light")
        ctk.set_default_color_theme("blue")

        self.title("TicketCounter")
        self.geometry("1460x920")
        self.minsize(1180, 760)
        self.configure(fg_color=APP_BG)

        self.data_service = TicketDataService()
        self.summary = pd.DataFrame()
        self.last_source_df = pd.DataFrame()
        self.current_source_name = "Excel / CSV"
        self.calculated_budget = 0.0

        self.file_path_var = ctk.StringVar()
        self.sheet_var = ctk.StringVar()
        self.api_url_var = ctk.StringVar()
        self.api_token_var = ctk.StringVar()
        self.status_var = ctk.StringVar(value="Hazır")
        self.source_type_var = ctk.StringVar(value="file")
        self.budget_var = ctk.StringVar(value="10000")

        self._build_layout()

    def _build_layout(self) -> None:
        self.grid_columnconfigure(0, weight=0)
        self.grid_columnconfigure(1, weight=1)
        self.grid_rowconfigure(0, weight=1)
        self.grid_rowconfigure(1, weight=0)

        self.sidebar = ctk.CTkScrollableFrame(self, fg_color=CARD_BG, corner_radius=22, width=390)
        self.sidebar.grid(row=0, column=0, sticky="nsew", padx=(22, 14), pady=22)
        self.sidebar.grid_columnconfigure(0, weight=1)

        self.content = ctk.CTkFrame(self, fg_color="transparent")
        self.content.grid(row=0, column=1, sticky="nsew", padx=(0, 22), pady=22)
        self.content.grid_columnconfigure(0, weight=1)
        self.content.grid_rowconfigure(2, weight=1)

        self.status_bar = ctk.CTkFrame(self, fg_color=CARD_BG, height=52, corner_radius=18)
        self.status_bar.grid(row=1, column=0, columnspan=2, sticky="ew", padx=22, pady=(0, 22))
        self.status_bar.grid_columnconfigure(0, weight=1)

        self._build_sidebar()
        self._build_header()
        self._build_stat_cards()
        self._build_main_panels()
        self._build_status_bar()

    def _build_sidebar(self) -> None:
        top = ctk.CTkFrame(self.sidebar, fg_color="transparent")
        top.grid(row=0, column=0, sticky="ew", padx=20, pady=(20, 10))
        top.grid_columnconfigure(0, weight=1)

        ctk.CTkLabel(top, text="TicketCounter", font=ctk.CTkFont(size=30, weight="bold"), text_color=TEXT_MAIN).grid(row=0, column=0, sticky="w")
        ctk.CTkLabel(
            top,
            text="Destek kayıtlarını say, yüzdeleri çıkar, bütçeyi sıcak ve temiz bir arayüzde bölüştür.",
            font=ctk.CTkFont(size=14),
            text_color=TEXT_MUTED,
            wraplength=320,
            justify="left",
        ).grid(row=1, column=0, sticky="w", pady=(8, 0))

        quick_card = self._card(self.sidebar)
        quick_card.grid(row=1, column=0, sticky="ew", padx=20, pady=10)
        quick_card.grid_columnconfigure((0, 1), weight=1)
        ctk.CTkLabel(quick_card, text="Hızlı İşlemler", font=ctk.CTkFont(size=18, weight="bold"), text_color=TEXT_MAIN).grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 14))
        ctk.CTkButton(quick_card, text="1) Analizi Çalıştır", height=48, fg_color=ACCENT, hover_color=ACCENT_HOVER, command=self.run).grid(row=1, column=0, columnspan=2, sticky="ew")
        ctk.CTkButton(quick_card, text="2) Yüzde + Ücret Hesapla", height=44, fg_color=ACCENT_2, hover_color=ACCENT_2_HOVER, command=self.calculate_budget).grid(row=2, column=0, columnspan=2, sticky="ew", pady=(10, 0))

        source_card = self._card(self.sidebar)
        source_card.grid(row=2, column=0, sticky="ew", padx=20, pady=10)
        source_card.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(source_card, text="Veri Kaynağı", font=ctk.CTkFont(size=18, weight="bold"), text_color=TEXT_MAIN).grid(row=0, column=0, sticky="w", pady=(0, 14))

        source_segment = ctk.CTkSegmentedButton(
            source_card,
            values=["Excel / CSV", "API"],
            command=self._on_source_changed,
            selected_color=ACCENT,
            selected_hover_color=ACCENT_HOVER,
            unselected_color=CARD_ALT,
            unselected_hover_color="#ead8c7",
            text_color=TEXT_MAIN,
        )
        source_segment.grid(row=1, column=0, sticky="ew", pady=(0, 12))
        source_segment.set("Excel / CSV")

        self.file_frame = ctk.CTkFrame(source_card, fg_color="transparent")
        self.file_frame.grid(row=2, column=0, sticky="ew")
        self.file_frame.grid_columnconfigure(0, weight=1)
        self._label(self.file_frame, "Dosya yolu").grid(row=0, column=0, sticky="w")
        path_row = ctk.CTkFrame(self.file_frame, fg_color="transparent")
        path_row.grid(row=1, column=0, sticky="ew", pady=(6, 10))
        path_row.grid_columnconfigure(0, weight=1)
        self.path_entry = ctk.CTkEntry(path_row, textvariable=self.file_path_var, height=42, fg_color=TABLE_BG, border_color=BORDER, text_color=TEXT_MAIN)
        self.path_entry.grid(row=0, column=0, sticky="ew", padx=(0, 8))
        ctk.CTkButton(path_row, text="Seç", width=78, fg_color=ACCENT, hover_color=ACCENT_HOVER, command=self.browse).grid(row=0, column=1)
        self._label(self.file_frame, "Excel sayfa adı (opsiyonel)").grid(row=2, column=0, sticky="w")
        self.sheet_entry = ctk.CTkEntry(self.file_frame, textvariable=self.sheet_var, height=42, fg_color=TABLE_BG, border_color=BORDER, text_color=TEXT_MAIN)
        self.sheet_entry.grid(row=3, column=0, sticky="ew", pady=(6, 0))

        self.api_frame = ctk.CTkFrame(source_card, fg_color="transparent")
        self.api_frame.grid_columnconfigure(0, weight=1)
        self._label(self.api_frame, "API URL").grid(row=0, column=0, sticky="w")
        self.api_url_entry = ctk.CTkEntry(self.api_frame, textvariable=self.api_url_var, height=42, fg_color=TABLE_BG, border_color=BORDER, text_color=TEXT_MAIN, placeholder_text="https://example.com/api/tickets")
        self.api_url_entry.grid(row=1, column=0, sticky="ew", pady=(6, 10))
        self._label(self.api_frame, "Bearer Token (opsiyonel)").grid(row=2, column=0, sticky="w")
        self.api_token_entry = ctk.CTkEntry(self.api_frame, textvariable=self.api_token_var, height=42, fg_color=TABLE_BG, border_color=BORDER, text_color=TEXT_MAIN, show="•")
        self.api_token_entry.grid(row=3, column=0, sticky="ew", pady=(6, 0))

        filter_card = self._card(self.sidebar)
        filter_card.grid(row=3, column=0, sticky="ew", padx=20, pady=10)
        filter_card.grid_columnconfigure((0, 1), weight=1)
        ctk.CTkLabel(filter_card, text="Tarih Aralığı", font=ctk.CTkFont(size=18, weight="bold"), text_color=TEXT_MAIN).grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 14))
        self._label(filter_card, "Başlangıç").grid(row=1, column=0, sticky="w")
        self._label(filter_card, "Bitiş").grid(row=1, column=1, sticky="w")
        self.start_cal = self._build_date_box(filter_card)
        self.start_cal.grid(row=2, column=0, sticky="ew", padx=(0, 8), pady=(6, 0))
        self.end_cal = self._build_date_box(filter_card)
        self.end_cal.grid(row=2, column=1, sticky="ew", pady=(6, 0))

        budget_card = self._card(self.sidebar)
        budget_card.grid(row=4, column=0, sticky="ew", padx=20, pady=10)
        budget_card.grid_columnconfigure((0, 1), weight=1)
        ctk.CTkLabel(budget_card, text="Bütçe Dağıtımı", font=ctk.CTkFont(size=18, weight="bold"), text_color=TEXT_MAIN).grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 14))
        self._label(budget_card, "Toplam bütçe (TL)").grid(row=1, column=0, columnspan=2, sticky="w")
        self.budget_entry = ctk.CTkEntry(budget_card, textvariable=self.budget_var, height=42, fg_color=TABLE_BG, border_color=BORDER, text_color=TEXT_MAIN)
        self.budget_entry.grid(row=2, column=0, columnspan=2, sticky="ew", pady=(6, 10))
        ctk.CTkButton(budget_card, text="Yüzde + Ücret Hesapla", height=44, fg_color=ACCENT_2, hover_color=ACCENT_2_HOVER, command=self.calculate_budget).grid(row=3, column=0, columnspan=2, sticky="ew")

        action_card = self._card(self.sidebar)
        action_card.grid(row=5, column=0, sticky="ew", padx=20, pady=10)
        action_card.grid_columnconfigure((0, 1), weight=1)
        ctk.CTkButton(action_card, text="CSV Dışa Aktar", height=42, fg_color=CARD_ALT, hover_color="#ead8c7", text_color=TEXT_MAIN, command=self.save).grid(row=0, column=0, sticky="ew", padx=(0, 8))
        ctk.CTkButton(action_card, text="Sıfırla", height=42, fg_color=CARD_ALT, hover_color="#ead8c7", text_color=TEXT_MAIN, command=self.reset_form).grid(row=0, column=1, sticky="ew")

        tip_card = self._card(self.sidebar)
        tip_card.grid(row=6, column=0, sticky="ew", padx=20, pady=(10, 20))
        ctk.CTkLabel(tip_card, text="Not", font=ctk.CTkFont(size=18, weight="bold"), text_color=TEXT_MAIN).pack(anchor="w")
        ctk.CTkLabel(
            tip_card,
            text="Butonlar görünmüyor sorununu çözmek için sol panel scrollable yapıldı. Önce Analizi Çalıştır, sonra istersen bütçe hesabı.",
            text_color=TEXT_MUTED,
            wraplength=300,
            justify="left",
            font=ctk.CTkFont(size=13),
        ).pack(anchor="w", pady=(10, 0))

        self._on_source_changed("Excel / CSV")

    def _build_date_box(self, parent):
        outer = ctk.CTkFrame(parent, fg_color=TABLE_BG, corner_radius=14, border_width=1, border_color=BORDER)
        date = DateEntry(
            outer,
            width=18,
            date_pattern="yyyy-mm-dd",
            background="#d8b08c",
            foreground=TEXT_MAIN,
            borderwidth=0,
            relief="flat",
            headersbackground="#e9d4c0",
            normalbackground="#fff8f0",
            weekendbackground="#fbf1e7",
            othermonthbackground="#f4e7d8",
            othermonthwebackground="#f4e7d8",
            selectbackground=ACCENT,
            selectforeground="#ffffff",
        )
        date.pack(fill="x", padx=8, pady=8)
        return outer

    def _extract_date(self, wrapper) -> str:
        for child in wrapper.winfo_children():
            if isinstance(child, DateEntry):
                return child.get_date().strftime("%Y-%m-%d")
        raise DataSourceError("Tarih alanı okunamadı.")

    def _build_header(self) -> None:
        header = ctk.CTkFrame(self.content, fg_color="transparent")
        header.grid(row=0, column=0, sticky="ew", pady=(0, 16))
        header.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(header, text="Destek Operasyon Görünümü", font=ctk.CTkFont(size=30, weight="bold"), text_color=TEXT_MAIN).grid(row=0, column=0, sticky="w")
        self.subtitle = ctk.CTkLabel(header, text="Önce soldaki büyük Analizi Çalıştır butonuna bas.", font=ctk.CTkFont(size=14), text_color=TEXT_MUTED)
        self.subtitle.grid(row=1, column=0, sticky="w", pady=(6, 0))

    def _build_stat_cards(self) -> None:
        stats = ctk.CTkFrame(self.content, fg_color="transparent")
        stats.grid(row=1, column=0, sticky="ew", pady=(0, 16))
        stats.grid_columnconfigure((0, 1, 2, 3), weight=1)
        self.total_card = self._stat_card(stats, 0, "Toplam Ticket", "0", ACCENT)
        self.people_card = self._stat_card(stats, 1, "Aktif Personel", "0", ACCENT_2)
        self.top_card = self._stat_card(stats, 2, "En Yüksek", "—", WARN)
        self.budget_card_value = self._stat_card(stats, 3, "Dağıtılan Bütçe", "0 TL", "#b9855b")

    def _build_main_panels(self) -> None:
        panels = ctk.CTkFrame(self.content, fg_color="transparent")
        panels.grid(row=2, column=0, sticky="nsew")
        panels.grid_columnconfigure(0, weight=1)
        panels.grid_columnconfigure(1, weight=1)
        panels.grid_rowconfigure(0, weight=1)

        table_card = self._card(panels)
        table_card.grid(row=0, column=0, sticky="nsew", padx=(0, 8))
        table_card.grid_rowconfigure(1, weight=1)
        table_card.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(table_card, text="Kullanıcı Dağılımı", font=ctk.CTkFont(size=20, weight="bold"), text_color=TEXT_MAIN).grid(row=0, column=0, sticky="w", pady=(0, 12))

        table_wrap = ctk.CTkFrame(table_card, fg_color=TABLE_BG, corner_radius=16)
        table_wrap.grid(row=1, column=0, sticky="nsew")
        table_wrap.grid_rowconfigure(0, weight=1)
        table_wrap.grid_columnconfigure(0, weight=1)

        style = ttk.Style()
        style.theme_use("default")
        style.configure("Treeview", background=TABLE_BG, foreground=TEXT_MAIN, fieldbackground=TABLE_BG, rowheight=36, borderwidth=0)
        style.configure("Treeview.Heading", background=TABLE_ALT, foreground=TEXT_MAIN, font=("Segoe UI", 11, "bold"), borderwidth=0)
        style.map("Treeview", background=[("selected", "#d8b08c")], foreground=[("selected", TEXT_MAIN)])

        self.tree = ttk.Treeview(table_wrap, columns=("user", "count", "percent", "amount"), show="headings")
        self.tree.heading("user", text="Kullanıcı")
        self.tree.heading("count", text="Ticket Adedi")
        self.tree.heading("percent", text="Yüzde")
        self.tree.heading("amount", text="Hakediş (TL)")
        self.tree.column("user", width=220, anchor="w")
        self.tree.column("count", width=120, anchor="center")
        self.tree.column("percent", width=120, anchor="center")
        self.tree.column("amount", width=150, anchor="center")
        self.tree.grid(row=0, column=0, sticky="nsew")
        scroll = ttk.Scrollbar(table_wrap, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscroll=scroll.set)
        scroll.grid(row=0, column=1, sticky="ns")

        chart_card = self._card(panels)
        chart_card.grid(row=0, column=1, sticky="nsew", padx=(8, 0))
        chart_card.grid_rowconfigure(1, weight=1)
        chart_card.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(chart_card, text="Görsel Dağılım", font=ctk.CTkFont(size=20, weight="bold"), text_color=TEXT_MAIN).grid(row=0, column=0, sticky="w", pady=(0, 12))
        self.chart_host = ctk.CTkFrame(chart_card, fg_color=CHART_BG, corner_radius=16)
        self.chart_host.grid(row=1, column=0, sticky="nsew")
        self.chart_host.grid_rowconfigure(0, weight=1)
        self.chart_host.grid_columnconfigure(0, weight=1)
        self.empty_chart_label = ctk.CTkLabel(self.chart_host, text="Henüz analiz yok. Soldaki büyük butonla başla.", text_color=TEXT_MUTED, font=ctk.CTkFont(size=14))
        self.empty_chart_label.grid(row=0, column=0)

    def _build_status_bar(self) -> None:
        ctk.CTkLabel(self.status_bar, textvariable=self.status_var, text_color=TEXT_MUTED, font=ctk.CTkFont(size=13)).grid(row=0, column=0, sticky="w", padx=18, pady=14)

    def _card(self, parent):
        return ctk.CTkFrame(parent, fg_color=CARD_BG, corner_radius=20, border_width=1, border_color=BORDER)

    def _label(self, parent, text: str):
        return ctk.CTkLabel(parent, text=text, text_color=TEXT_MUTED, font=ctk.CTkFont(size=13))

    def _stat_card(self, parent, column: int, title: str, value: str, accent_color: str):
        card = ctk.CTkFrame(parent, fg_color=CARD_BG, corner_radius=20, border_width=1, border_color=BORDER)
        card.grid(row=0, column=column, sticky="ew", padx=(0 if column == 0 else 8, 0 if column == 3 else 8))
        stripe = ctk.CTkFrame(card, fg_color=accent_color, height=6, corner_radius=999)
        stripe.pack(fill="x", padx=16, pady=(16, 10))
        ctk.CTkLabel(card, text=title, text_color=TEXT_MUTED, font=ctk.CTkFont(size=13)).pack(anchor="w", padx=16)
        value_lbl = ctk.CTkLabel(card, text=value, text_color=TEXT_MAIN, font=ctk.CTkFont(size=25, weight="bold"))
        value_lbl.pack(anchor="w", padx=16, pady=(8, 16))
        return value_lbl

    def _on_source_changed(self, value: str) -> None:
        self.source_type_var.set("file" if value == "Excel / CSV" else "api")
        if self.source_type_var.get() == "file":
            self.api_frame.grid_remove()
            self.file_frame.grid(row=2, column=0, sticky="ew")
            self.status_var.set("Excel / CSV modu aktif")
        else:
            self.file_frame.grid_remove()
            self.api_frame.grid(row=2, column=0, sticky="ew")
            self.status_var.set("API modu aktif")

    def browse(self) -> None:
        path = filedialog.askopenfilename(filetypes=[("Excel / CSV", "*.xlsx;*.xls;*.csv")])
        if path:
            self.file_path_var.set(path)
            self.status_var.set(f"Dosya seçildi: {Path(path).name}")

    def run(self) -> None:
        try:
            payload = self._load_payload()
            start = self._extract_date(self.start_cal)
            end = self._extract_date(self.end_cal)
            summary = self.data_service.summarize_assignees(payload.df, start, end)

            if summary.empty:
                summary = self.data_service.summarize_assignees(payload.df, None, None)
                if summary.empty:
                    raise DataSourceError("Sonuç üretilemedi. Assignee/atanan kullanıcı alanı boş olabilir.")
                messagebox.showinfo("Bilgi", "Seçilen tarih aralığında sonuç çıkmadı; tüm kayıtlar gösteriliyor.")

            self.summary = summary.reset_index(drop=True)
            self.last_source_df = payload.df.copy()
            self.current_source_name = payload.source_label
            self.calculated_budget = 0.0
            self._render_summary()
            self.status_var.set(f"Analiz tamamlandı • Kaynak: {payload.source_label}")
            self.subtitle.configure(text=f"Kaynak: {payload.source_label} • Tarih filtresi: {start} → {end}")
            self.budget_card_value.configure(text="0 TL")
        except Exception as exc:
            self.status_var.set("Hata oluştu")
            messagebox.showerror("Hata", str(exc))

    def calculate_budget(self) -> None:
        if self.summary.empty:
            messagebox.showinfo("Bilgi", "Önce Analizi Çalıştır butonuna bas.")
            return
        try:
            raw_budget = self.budget_var.get().strip().replace("₺", "").replace("TL", "").replace("tl", "").replace(" ", "").replace(",", ".")
            budget = float(raw_budget)
            self.summary = self.data_service.apply_budget_distribution(self.summary, budget)
            self.calculated_budget = budget
            self._render_summary()
            self.budget_card_value.configure(text=self._format_currency(budget))
            self.status_var.set(f"Bütçe dağıtımı hesaplandı • {budget:.2f} TL")
        except Exception as exc:
            messagebox.showerror("Hata", str(exc))

    def _load_payload(self) -> SourcePayload:
        if self.source_type_var.get() == "file":
            path = self.file_path_var.get().strip()
            if not path:
                raise DataSourceError("Lütfen bir Excel/CSV dosyası seç.")
            return self.data_service.load_from_file(Path(path), self.sheet_var.get().strip() or None)

        return self.data_service.load_from_api(self.api_url_var.get().strip(), self.api_token_var.get().strip())

    def _render_summary(self) -> None:
        for item in self.tree.get_children():
            self.tree.delete(item)

        has_budget = "Yüzde" in self.summary.columns and "Hakediş (TL)" in self.summary.columns
        for _, row in self.summary.iterrows():
            percent = f"%{float(row['Yüzde']):.2f}" if has_budget else "—"
            amount = self._format_currency(float(row["Hakediş (TL)"])) if has_budget else "—"
            self.tree.insert("", "end", values=(row["Kullanıcı"], int(row["Ticket Adedi"]), percent, amount))

        stats = self.data_service.compute_stats(self.summary)
        self.total_card.configure(text=str(stats["total"]))
        self.people_card.configure(text=str(stats["people"]))
        self.top_card.configure(text=f"{stats['top_name']} ({stats['top_count']})")
        self._draw_chart()

    def _draw_chart(self) -> None:
        for child in self.chart_host.winfo_children():
            child.destroy()

        if self.summary.empty:
            self.empty_chart_label = ctk.CTkLabel(self.chart_host, text="Henüz analiz yok. Soldaki büyük butonla başla.", text_color=TEXT_MUTED)
            self.empty_chart_label.grid(row=0, column=0)
            return

        counts = pd.to_numeric(self.summary["Ticket Adedi"], errors="coerce").fillna(0).astype(float)
        labels = self.summary["Kullanıcı"].astype(str).tolist()
        total = int(counts.sum())

        fig = plt.Figure(figsize=(5.8, 4.2), dpi=110)
        fig.patch.set_facecolor(CHART_BG)
        ax = fig.add_subplot(111)
        ax.set_facecolor(CHART_BG)
        colors = ["#c7794b", "#d59b4c", "#9f6c50", "#b9855b", "#e0b58b", "#d48f6a", "#b97a56", "#e3c7a4"]
        _, _, autotexts = ax.pie(
            counts.values,
            labels=labels,
            autopct=lambda p: f"{p:.1f}%" if p > 4 else "",
            startangle=90,
            colors=colors[: len(labels)],
            wedgeprops={"linewidth": 2, "edgecolor": CHART_BG},
            textprops={"color": TEXT_MAIN, "fontsize": 10},
            pctdistance=0.8,
            labeldistance=1.05,
        )
        for at in autotexts:
            at.set_color(TEXT_MAIN)
            at.set_fontsize(9)
        ax.axis("equal")
        ax.set_title(f"Toplam Ticket: {total}", color=TEXT_MAIN, pad=18, fontsize=15)

        canvas = FigureCanvasTkAgg(fig, master=self.chart_host)
        canvas.draw()
        canvas.get_tk_widget().grid(row=0, column=0, sticky="nsew", padx=6, pady=6)

    def save(self) -> None:
        if self.summary.empty:
            messagebox.showinfo("Bilgi", "Önce Analizi Çalıştır butonuna bas.")
            return
        path = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV", "*.csv")])
        if not path:
            return
        self.summary.to_csv(path, index=False)
        self.status_var.set(f"CSV kaydedildi: {Path(path).name}")
        messagebox.showinfo("Kaydedildi", path)

    def reset_form(self) -> None:
        self.file_path_var.set("")
        self.sheet_var.set("")
        self.api_url_var.set("")
        self.api_token_var.set("")
        self.budget_var.set("10000")
        self.summary = pd.DataFrame()
        self.last_source_df = pd.DataFrame()
        self.current_source_name = "Excel / CSV"
        self.calculated_budget = 0.0
        self.subtitle.configure(text="Önce soldaki büyük Analizi Çalıştır butonuna bas.")
        self.status_var.set("Form sıfırlandı")
        self.total_card.configure(text="0")
        self.people_card.configure(text="0")
        self.top_card.configure(text="—")
        self.budget_card_value.configure(text="0 TL")
        for item in self.tree.get_children():
            self.tree.delete(item)
        self._draw_chart()

    def _format_currency(self, value: float) -> str:
        return f"{value:,.2f} TL".replace(",", "X").replace(".", ",").replace("X", ".")


if __name__ == "__main__":
    App().mainloop()
