# TicketCounter

## Overview
TicketCounter is a Python desktop analytics tool for importing support ticket records from Excel or CSV files and turning them into quick workload summaries. It helps teams see how tickets are distributed across assigned support staff, apply date-based filtering, review charts and summary cards, and export results for reporting.

The project is intentionally lightweight: it focuses on practical internal analytics rather than a large backend service. That makes it a good example of desktop UI work, data ingestion, and applied reporting with Python.

## Features
- Import ticket datasets from `.xlsx`, `.xls`, and `.csv` files
- Normalize supported column names from exported support data
- Filter records by date range
- Aggregate ticket counts by assigned support staff
- Display summary cards, data tables, and pie charts in a desktop interface
- Calculate budget-based distribution over ticket volume
- Export analysis results as CSV
- Keep a PyInstaller spec file for Windows executable packaging

## Tech Stack
- Python
- Tkinter / CustomTkinter
- Pandas
- Matplotlib
- tkcalendar
- Requests
- OpenPyXL / xlrd / lxml
- PyInstaller

## Supported Data Sources
TicketCounter currently supports file-based imports:
- Excel `.xlsx`
- Legacy Excel `.xls`
- CSV `.csv`

The UI also includes an API ingestion path for JSON payloads. It is designed as a generic extension point and does not contain any real endpoint credentials or private integrations.

## Installation
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python src/app_tk.py
```

Windows activation command:

```bash
.venv\Scripts\activate
```

To build a Windows executable with the included spec file:

```bash
pyinstaller SupportSayaci.spec
```

## Usage
1. Launch the desktop app.
2. Select an Excel or CSV export file.
3. Optionally choose a date range.
4. Run the analysis to calculate ticket distribution by assignee.
5. Review summary cards, the results table, and the chart.
6. Export the generated summary if needed.

## Example Input Format
An example dataset is available at `examples/sample_tickets.csv`.

Expected or supported source columns include:
- `Atanan Destek Personeli`
- `Oluşturma Tarihi`
- `Güncelleme Tarihi`
- `Kapatan Kullanıcı`

Sample row:

```csv
Ticket ID,Atanan Destek Personeli,Oluşturma Tarihi,Güncelleme Tarihi,Kapatan Kullanıcı,Durum
TCK-001,Alice Carter,2026-05-01 09:15:00,2026-05-01 11:30:00,Brian Stone,Closed
```

## Exported Output
The exported summary file contains assignee-based aggregated results, including ticket counts and, when used, budget distribution fields such as percentage and payout allocation.

## Project Status
This repository is a cleaned-up portfolio version of the application. The current focus is keeping the project runnable, understandable, and easy to review for desktop/data-tooling use cases.

## Future Improvements
- Add automated tests for data parsing and aggregation logic
- Improve import previews and validation messages
- Add richer chart options and filtering controls
- Separate packaging dependencies from runtime dependencies
- Add sample screenshots or a short demo GIF
