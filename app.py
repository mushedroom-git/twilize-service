"""
Twilize Microservice — Generates Tableau .twbx workbooks from TableauSpec JSON.
Deploy on Railway, Fly.io, or Render.

Uses the REAL twilize API (tested and confirmed working):
  - build_dashboard_from_csv()
  - TWBEditor with clear_existing_content=False
  - ChartSuggestion / DashboardSuggestion / ShelfAssignment
"""

import base64
import csv
import os
import random
import tempfile
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Twilize imports — REAL API surface
# ---------------------------------------------------------------------------
try:
    from twilize import build_dashboard_from_csv
    from twilize.chart_suggester import (
        ChartSuggestion,
        DashboardSuggestion,
        ShelfAssignment,
    )
    from twilize.twb_editor import TWBEditor
    TWILIZE_AVAILABLE = True
except ImportError as e:
    TWILIZE_AVAILABLE = False
    TWILIZE_IMPORT_ERROR = str(e)

app = FastAPI(title="Twilize Workbook Service", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Models — matching Lovable's TableauSpec
# ---------------------------------------------------------------------------

class TableauShelf(BaseModel):
    field: str
    shelf: str  # columns, rows, color, text, wedge_size, size, detail
    aggregation: str = ""

class TableauChart(BaseModel):
    type: str  # Bar, Line, Pie, Circle, Area, Square, Text, Map
    title: str
    shelves: list[TableauShelf]
    page: str = ""

class TableauKpi(BaseModel):
    name: str
    value: Optional[str] = None
    color: Optional[str] = None
    page: str = ""

class TableauFilter(BaseModel):
    field: str
    defaultValue: Optional[str] = None

class TableauColumn(BaseModel):
    name: str
    type: str  # string, integer, real, date
    role: str  # dimension, measure

class TableauBrand(BaseModel):
    template: str = "Standard_SE"
    primaryColor: Optional[str] = "#3DCD58"
    fontFamily: Optional[str] = "Arial"
    backgroundColor: Optional[str] = "#FFFFFF"
    colors: list[str] = [
        "#3DCD58", "#009530", "#0069B4", "#FF7900",
        "#E6007E", "#8C8C8C", "#00A19A", "#FFB900",
    ]
    color_dimension_map: dict[str, str] = {}

class TableauSpec(BaseModel):
    title: str
    pages: list[str] = []
    columns: list[TableauColumn] = []
    charts: list[TableauChart] = []
    kpis: list[TableauKpi] = []
    filters: list[TableauFilter] = []
    brand: TableauBrand = TableauBrand()
    sampleRowCount: int = 200

class GenerateRequest(BaseModel):
    spec: TableauSpec
    sample_csv: Optional[str] = None

# ---------------------------------------------------------------------------
# Chart type mapping: Lovable type → twilize mark type
# ---------------------------------------------------------------------------

CHART_TYPE_MAP = {
    "bar": "Bar", "Bar": "Bar",
    "line": "Line", "Line": "Line",
    "pie": "Pie", "Pie": "Pie",
    "circle": "Circle", "Circle": "Circle",
    "area": "Area", "Area": "Area",
    "square": "Square", "Square": "Square",
    "text": "Text", "Text": "Text",
    "map": "Map", "Map": "Map",
}

# ---------------------------------------------------------------------------
# Shelf mapping: Lovable shelf names → twilize shelf names
# ---------------------------------------------------------------------------

SHELF_MAP = {
    "columns": "columns",
    "rows": "rows",
    "color": "color",
    "size": "size",
    "label": "text",
    "text": "text",
    "detail": "detail",
    "wedge_size": "wedge_size",
}

# ---------------------------------------------------------------------------
# Sample CSV generator
# ---------------------------------------------------------------------------

_STRING_SAMPLES = {
    "region":    ["EMEA", "NAM", "APAC", "LATAM", "MEA"],
    "country":   ["Germany", "US", "China", "India", "Brazil", "France", "Japan"],
    "plant":     ["Plant-EU01", "Plant-US02", "Plant-CN03", "Plant-IN04"],
    "category":  ["Category A", "Category B", "Category C", "Category D"],
    "type":      ["Type A", "Type B", "Type C", "Type D"],
    "status":    ["Active", "Inactive", "Pending", "Closed"],
    "segment":   ["Consumer", "Corporate", "Home Office"],
}


def _sample_string(col_name: str) -> str:
    name_lower = col_name.lower()
    for key, vals in _STRING_SAMPLES.items():
        if key in name_lower:
            return random.choice(vals)
    if "id" in name_lower:
        return f"ID-{random.randint(10000, 99999)}"
    if "name" in name_lower:
        return random.choice(["Alice", "Bob", "Carlos", "Diana", "Erik"]) + " " + \
               random.choice(["Smith", "Garcia", "Mueller", "Chen", "Patel"])
    if "material" in name_lower or "product" in name_lower:
        return f"MAT-{random.randint(1000, 9999)}"
    return f"Val_{random.randint(1, 20)}"


def _sample_value(col: TableauColumn, row: int):
    dt = col.type.lower()
    if dt == "string":
        return _sample_string(col.name)
    elif dt == "integer":
        return random.randint(1, 10000)
    elif dt in ("real", "float", "number"):
        return round(random.uniform(100, 500000), 2)
    elif dt == "date":
        base = datetime(2024, 1, 1)
        return (base + timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d")
    return f"val_{row}"


def _generate_sample_csv(columns: list[TableauColumn], num_rows: int, path: str):
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[c.name for c in columns])
        writer.writeheader()
        for i in range(num_rows):
            writer.writerow({c.name: _sample_value(c, i) for c in columns})


# ---------------------------------------------------------------------------
# Workbook generation
# ---------------------------------------------------------------------------

def build_workbook(spec: TableauSpec, sample_csv: Optional[str]) -> bytes:
    if not TWILIZE_AVAILABLE:
        raise HTTPException(
            status_code=500,
            detail=f"twilize failed to import: {TWILIZE_IMPORT_ERROR}",
        )

    with tempfile.TemporaryDirectory() as tmp_dir:
        csv_path = os.path.join(tmp_dir, "data.csv")
        temp_twbx = os.path.join(tmp_dir, "temp.twbx")
        final_twbx = os.path.join(tmp_dir, "output.twbx")

        # ---- 1. Write CSV ----
        if sample_csv and sample_csv.strip():
            Path(csv_path).write_text(sample_csv, encoding="utf-8")
        else:
            _generate_sample_csv(spec.columns, spec.sampleRowCount, csv_path)

        # ---- 2. Build ChartSuggestions ----
        charts = []
        for chart in spec.charts:
            shelves = []
            for s in chart.shelves:
                twilize_shelf = SHELF_MAP.get(s.shelf, s.shelf)
                shelves.append(
                    ShelfAssignment(s.field, twilize_shelf, s.aggregation)
                )
            charts.append(
                ChartSuggestion(
                    chart_type=CHART_TYPE_MAP.get(chart.type, "Bar"),
                    title=chart.title,
                    shelves=shelves,
                    sort_descending="",
                )
            )

        suggestion = DashboardSuggestion(
            title=spec.title,
            charts=charts,
        )

        # ---- 3. Build with twilize ----
        build_dashboard_from_csv(
            csv_path=csv_path,
            output_path=temp_twbx,
            dashboard_title=spec.title,
            suggestion=suggestion,
        )

        # ---- 4. Apply branding ----
        editor = TWBEditor(temp_twbx, clear_existing_content=False)

        # Register brand palette
        if spec.brand.colors:
            editor.apply_color_palette(
                colors=spec.brand.colors, custom_name="brand-palette"
            )

        # Style dashboards
        bg_color = spec.brand.backgroundColor or "#FFFFFF"
        font_family = spec.brand.fontFamily or "Arial"
        for db in editor.list_dashboards():
            editor.apply_dashboard_theme(
                dashboard_name=db["name"],
                background_color=bg_color,
                font_family=font_family,
                title_font_size="11",
            )

        # Style worksheets + apply mark colors
        mark_color = spec.brand.primaryColor or "#3DCD58"
        for ws_name in editor.list_worksheets():
            try:
                has_color_dim = any(
                    any(s.shelf == "color" for s in c.shelves)
                    for c in spec.charts
                    if c.title == ws_name
                )

                if has_color_dim and spec.brand.color_dimension_map:
                    editor.configure_chart(
                        worksheet_name=ws_name,
                        color_map=spec.brand.color_dimension_map,
                    )

                editor.configure_worksheet_style(
                    worksheet_name=ws_name,
                    background_color=bg_color,
                    hide_gridlines=True,
                    hide_zeroline=True,
                    pane_mark_style=(
                        {"mark-color": mark_color}
                        if not has_color_dim
                        else None
                    ),
                )
            except Exception:
                pass

        editor.save(final_twbx)

        # ---- 5. Return bytes ----
        return Path(final_twbx).read_bytes()


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/health")
async def health():
    result = {"status": "ok", "twilize_available": TWILIZE_AVAILABLE}
    if not TWILIZE_AVAILABLE:
        result["error"] = TWILIZE_IMPORT_ERROR
    return result


@app.post("/generate")
async def generate(request: GenerateRequest):
    try:
        twbx_bytes = build_workbook(request.spec, request.sample_csv)
        twbx_base64 = base64.b64encode(twbx_bytes).decode("utf-8")
        return {"twbx_base64": twbx_base64}
    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
