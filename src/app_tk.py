import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from tkcalendar import DateEntry
from pathlib import Path
import pandas as pd
import matplotlib
matplotlib.use("TkAgg")
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import xlrd
import json

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

def _read_table(path: Path, sheet_name: str | None) -> pd.DataFrame:

    suf = path.suffix.lower()
    print(f"[DEBUG] Okunacak dosya: {path} (suffix={suf})")

    def _clean_df(df: pd.DataFrame, how: str) -> pd.DataFrame:
        print(f"[DEBUG] OK -> {how}")
        df.columns = (
            df.columns.astype(str)
              .str.replace("\ufeff", "", regex=False)  # BOM
              .str.strip()
        )
        for c in df.columns:
            df[c] = df[c].astype(str).str.replace("\ufeff", "", regex=False).str.strip()
        return df.fillna("")

    if suf == ".xlsx":
        df = pd.read_excel(path, dtype=str, sheet_name=sheet_name or 0, engine="openpyxl")
        return _clean_df(df, "openpyxl (xlsx)")

    if suf == ".xls":
        try:

            df = pd.read_excel(path, dtype=str, sheet_name=sheet_name or 0, engine="xlrd")
            return _clean_df(df, "xlrd (binary xls)")
        except Exception as e:
            print("[DEBUG] xlrd (binary xls) başarısız:", e)

        try:
            with open(path, "rb") as f:
                head = f.read(2048)
            head_txt = head.decode("utf-8", errors="ignore").lower()
            if "<?xml" in head_txt and ("spreadsheet" in head_txt or "urn:schemas-microsoft-com:office:spreadsheet" in head_txt):
                from lxml import etree
                ns = {"ss": "urn:schemas-microsoft-com:office:spreadsheet"}

                tree = etree.parse(str(path))
                ws = tree.xpath("//ss:Worksheet[1]//ss:Table//ss:Row", namespaces=ns)
                rows = []
                max_len = 0
                for r in ws:
                    vals = [ (d.text or "") for d in r.xpath("./ss:Cell/ss:Data", namespaces=ns) ]
                    rows.append(vals)
                    max_len = max(max_len, len(vals))

                if not rows:
                    raise ValueError("XML Spreadsheet içeriği boş görünüyor.")

                for i in range(len(rows)):
                    if len(rows[i]) < max_len:
                        rows[i] += [""] * (max_len - len(rows[i]))

                headers = [str(h or "").strip() for h in rows[0]]
                data = rows[1:]
                df = pd.DataFrame(data, columns=headers)
                return _clean_df(df, "lxml (Excel 2003 XML)")

        except Exception as e:
            print("[DEBUG] XML parse denemesi başarısız:", e)

        for enc in ("utf-8-sig", "cp1254", "latin1"):
            for sep in ("|", ";", ",", "\t", None):
                try:
                    df = pd.read_csv(path, dtype=str, sep=sep, encoding=enc, engine="python")
                    return _clean_df(df, f"read_csv fallback (sep={repr(sep)}, enc={enc})")
                except Exception as e:
                    print(f"[DEBUG] read_csv fallback fail (sep={repr(sep)}, enc={enc}): {e}")

        raise ValueError(".xls dosyası okunamadı (binary değil, XML de parse edilemedi).")

    if suf == ".csv":
        df = pd.read_csv(path, dtype=str)
        return _clean_df(df, "csv")

    raise ValueError("Lütfen XLSX/XLS/CSV verin.")



def _apply_mapping(df: pd.DataFrame) -> pd.DataFrame:
    if COLUMN_MAP_OVERRIDE:
        df = df.rename(columns=COLUMN_MAP_OVERRIDE)

    for c in ["closed_by", "status_changed_at", "created_at", "assignee"]:
        if c not in df.columns:
            df[c] = ""

    import json

    def parse_assignee(val):
        if not val:
            return ""
        s = str(val).strip()
        if s.startswith("{") and s.endswith("}"):
            try:
                obj = json.loads(s)
                if "adi_soyadi" in obj and str(obj["adi_soyadi"]).strip():
                    return str(obj["adi_soyadi"]).strip()
            except Exception as e:
                print("❌ JSON parse hatası:", e, "VAL:", val)
        return s


    df["assignee"] = df["assignee"].apply(parse_assignee).astype(str).str.strip()

    # 🔹 DEBUG: İlk 10 satırı ekrana yazdıralım
    print("👉 Assignee kolonunun ilk 10 satırı:")
    print(df["assignee"].head(10).to_list())

    return df


"""def _apply_mapping(df: pd.DataFrame) -> pd.DataFrame:
    if COLUMN_MAP_OVERRIDE:
        df = df.rename(columns=COLUMN_MAP_OVERRIDE)

    # Gerekli kolonları ekle (yoksa boş string)
    for c in ["closed_by", "status_changed_at", "created_at", "assignee"]:
        if c not in df.columns:
            df[c] = ""

    def parse_assignee(val):
        if not val:
            return ""
        s = str(val).strip()
        if s.startswith("{") and s.endswith("}"):
            try:
                obj = json.loads(s)
                if "adi_soyadi" in obj and str(obj["adi_soyadi"]).strip():
                    return str(obj["adi_soyadi"]).strip()
            except Exception as e:
                print("❌ JSON parse hatası:", e, "VAL:", val)
        return s

    df["assignee"] = df["assignee"].apply(parse_assignee).astype(str).str.strip()
    return df"""


def _count_closed_by_in_range(df: pd.DataFrame, start: str | None, end: str | None) -> pd.DataFrame:
    df = _apply_mapping(df)

    def pick_best_date_series(df: pd.DataFrame):
        cands = []
        if "status_changed_at" in df.columns: cands.append("status_changed_at")
        if "created_at" in df.columns:       cands.append("created_at")
        best_col, best_s, best_ok = None, None, -1

        for col in cands:
            raw = df[col]

            s = pd.to_datetime(raw, errors="coerce")
            ok = s.notna().sum()

            if ok < max(1, int(len(raw) * 0.2)):  # %20'den azsa deneyelim
                nums = pd.to_numeric(raw, errors="coerce")
                s_alt = pd.to_datetime(nums, unit="D", origin="1899-12-30", errors="coerce")
                ok_alt = s_alt.notna().sum()
                if ok_alt > ok:
                    s, ok = s_alt, ok_alt

            if ok > best_ok:
                best_col, best_s, best_ok = col, s, ok

        return best_col, best_s

    date_col, s = pick_best_date_series(df)

    if start or end:
        m = pd.Series(True, index=df.index)
        if start:
            start_ts = pd.to_datetime(start).floor("D")
            m &= s >= start_ts
        if end:
            end_ts = pd.to_datetime(end).floor("D") + pd.Timedelta(days=1)  # gün sonu dahil
            m &= s < end_ts
        df = df[m]

    df = df[df["assignee"].astype(str).str.strip().str.len() > 0]

    out = (
        df.groupby("assignee", as_index=False)
          .size()
          .rename(columns={"assignee": "Kullanıcı", "size": "Kapatma Adedi"})
          .sort_values("Kapatma Adedi", ascending=False)
    )
    return out


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("🎫 Destek Kapatma Sayacı (Tkinter)")
        self.geometry("900x560")
        self.minsize(800, 480)

        # sol panel: kontroller
        left = ttk.Frame(self, padding=10)
        left.pack(side=tk.LEFT, fill=tk.Y)

        ttk.Label(left, text="Veri Dosyası (XLSX/XLS/CSV):").pack(anchor="w")
        path_row = ttk.Frame(left); path_row.pack(fill=tk.X, pady=2)
        self.path_var = tk.StringVar()
        ttk.Entry(path_row, textvariable=self.path_var, width=40).pack(side=tk.LEFT, padx=(0,5))
        ttk.Button(path_row, text="Gözat", command=self.browse).pack(side=tk.LEFT)

        ttk.Label(left, text="Excel sayfa adı (opsiyonel):").pack(anchor="w", pady=(6,0))
        self.sheet_var = tk.StringVar()
        ttk.Entry(left, textvariable=self.sheet_var, width=24).pack(anchor="w")

        ttk.Label(left, text="Başlangıç Tarihi:").pack(anchor="w", pady=(6,0))
        self.start_cal = DateEntry(left, width=16, date_pattern="yyyy-mm-dd")
        self.start_cal.pack(anchor="w")

        ttk.Label(left, text="Bitiş Tarihi:").pack(anchor="w", pady=(6,0))
        self.end_cal = DateEntry(left, width=16, date_pattern="yyyy-mm-dd")
        self.end_cal.pack(anchor="w")


        btns = ttk.Frame(left); btns.pack(pady=10)
        ttk.Button(btns, text="Analiz", command=self.run).pack(side=tk.LEFT, padx=(0,6))
        self.save_btn = ttk.Button(btns, text="CSV Kaydet", command=self.save, state=tk.DISABLED)
        self.save_btn.pack(side=tk.LEFT)

        # sağ panel: tablo + grafik
        right = ttk.Frame(self, padding=10)
        right.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True)

        ttk.Label(right, text="Kullanıcı Bazında Kapatma Sayısı").pack(anchor="w")
        self.tree = ttk.Treeview(right, columns=("k","n"), show="headings", height=10)
        self.tree.heading("k", text="Kullanıcı")
        self.tree.heading("n", text="Kapatma Adedi")
        self.tree.pack(fill=tk.X, pady=5)

        ttk.Label(right, text="Pasta Grafik (yüzdeli)").pack(anchor="w")
        self.canvas = tk.Canvas(right, width=600, height=340)
        self.canvas.pack(fill=tk.BOTH, expand=True)

        self.summary = pd.DataFrame()

    def browse(self):
        p = filedialog.askopenfilename(filetypes=[("Excel/CSV","*.xlsx;*.xls;*.csv")])
        if p:
            self.path_var.set(p)

    def run(self):
        p = self.path_var.get().strip()
        if not p:
            messagebox.showerror("Hata", "Lütfen bir dosya seçin.")
            return
        try:
            df = _read_table(Path(p), self.sheet_var.get().strip() or None)
            self.summary = _count_closed_by_in_range(
                df,
                self.start_cal.get_date(),
                self.end_cal.get_date()
        )
            # eğer boşsa (muhtemelen aralık bugüne dar) => TÜM TARİHLER
            if self.summary.empty:
                self.summary = _count_closed_by_in_range(df, None, None)
                if self.summary.empty:
                    messagebox.showinfo(
                        "Bilgi",
                        "Kayıt bulunamadı.\n\nMuhtemel nedenler:\n"
                        "- 'Atanan Destek Personeli' alanı boş\n"
                        "- Dosyada satır yok"
                    )
                else:
                    messagebox.showinfo(
                        "Bilgi",
                        "Seçtiğin tarih aralığında sonuç yoktu.\nTüm tarihler için gösteriyorum."
                    )

        except Exception as e:
            messagebox.showerror("Hata", str(e))
            return

        for i in self.tree.get_children():
            self.tree.delete(i)
        for _, row in self.summary.iterrows():
            self.tree.insert("", tk.END, values=(row["Kullanıcı"], int(row["Kapatma Adedi"])))

        self.save_btn.config(state=(tk.NORMAL if not self.summary.empty else tk.DISABLED))

        # grafiği çiz
        self.draw_pie()

    def draw_pie(self):
        for child in self.canvas.winfo_children():
            child.destroy()
        if self.summary.empty:
            return
        fig = plt.Figure(figsize=(5.8,3.2))
        ax = fig.add_subplot(111)
        vals = self.summary["Kapatma Adedi"].values
        labels = self.summary["Kullanıcı"].values
        ax.pie(vals, labels=labels, autopct="%1.1f%%", startangle=90)
        ax.axis("equal")
        agg = FigureCanvasTkAgg(fig, master=self.canvas)
        agg.draw()
        agg.get_tk_widget().pack(fill="both", expand=True)

    def save(self):
        if self.summary.empty:
            return
        p = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV","*.csv")])
        if not p: 
            return
        self.summary.to_csv(p, index=False)
        messagebox.showinfo("Kaydedildi", p)

if __name__ == "__main__":
    App().mainloop()
