"""
Twilize Microservice — Generates Tableau .twbx workbooks from TableauSpec JSON.
Deploy on Railway, Fly.io, or Render.
"""

import base64
import csv
import io
import json
import os
import tempfile
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Twilize imports — install via `pip install twilize`
# If twilize is not yet published on PyPI, adjust the import path accordingly.
# ---------------------------------------------------------------------------
try:
    from twilize import TWBBuilder, TWBEditor, ChartType, DashboardSuggestion, ChartSuggestion
    TWILIZE_AVAILABLE = True
except ImportError:
    TWILIZE_AVAILABLE = False

app = FastAPI(title="Twilize Workbook Service", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class TableauShelf(BaseModel):
    field: str
    shelf: str

class TableauChart(BaseModel):
    type: str
    title: str
    shelves: list[TableauShelf]
    page: str

class TableauKpi(BaseModel):
    name: str
    value: Optional[str] = None
    color: Optional[str] = None
    page: str

class TableauFilter(BaseModel):
    field: str
    defaultValue: Optional[str] = None

class TableauColumn(BaseModel):
    name: str
    type: str
    role: str

class TableauBrand(BaseModel):
    template: str = "Standard_SE"
    primaryColor: Optional[str] = None
    fontFamily: Optional[str] = None

class TableauSpec(BaseModel):
    title: str
    pages: list[str]
    columns: list[TableauColumn]
    charts: list[TableauChart]
    kpis: list[TableauKpi]
    filters: list[TableauFilter]
    brand: TableauBrand
    sampleRowCount: int = 5

class GenerateRequest(BaseModel):
    spec: TableauSpec
    sample_csv: str

# ---------------------------------------------------------------------------
# Chart type mapping
# ---------------------------------------------------------------------------

CHART_TYPE_MAP: dict[str, "ChartType"] = {}

def _init_chart_map():
    if not TWILIZE_AVAILABLE:
        return
    global CHART_TYPE_MAP
    CHART_TYPE_MAP = {
        "Bar": ChartType.BAR,
        "Line": ChartType.LINE,
        "Pie": ChartType.PIE,
        "Circle": ChartType.SCATTER,
        "Area": ChartType.AREA,
        "Square": ChartType.HEATMAP,
        "Text": ChartType.TABLE,
        "Map": ChartType.MAP,
    }

_init_chart_map()

# ---------------------------------------------------------------------------
# Workbook generation
# ---------------------------------------------------------------------------

def build_workbook(spec: TableauSpec, sample_csv: str) -> bytes:
    """Build a .twbx file from a TableauSpec and sample CSV data."""
    if not TWILIZE_AVAILABLE:
        raise HTTPException(
            status_code=500,
            detail="twilize library is not installed. Install it with: pip install twilize",
        )

    with tempfile.TemporaryDirectory() as tmp_dir:
        # Write sample CSV to temp file
        csv_path = os.path.join(tmp_dir, "data.csv")
        with open(csv_path, "w", newline="") as f:
            f.write(sample_csv)

        output_path = os.path.join(tmp_dir, "output.twbx")

        # Build chart suggestions from spec
        chart_suggestions = []
        for chart in spec.charts:
            chart_type = CHART_TYPE_MAP.get(chart.type, ChartType.BAR)
            shelves_dict = {s.shelf: s.field for s in chart.shelves}
            chart_suggestions.append(
                ChartSuggestion(
                    chart_type=chart_type,
                    title=chart.title,
                    columns_field=shelves_dict.get("columns"),
                    rows_field=shelves_dict.get("rows"),
                    color_field=shelves_dict.get("color"),
                    size_field=shelves_dict.get("size"),
                    label_field=shelves_dict.get("label"),
                    detail_field=shelves_dict.get("detail"),
                    page=chart.page,
                )
            )

        # Build dashboard suggestion
        dashboard = DashboardSuggestion(
            title=spec.title,
            pages=spec.pages,
            charts=chart_suggestions,
            filters=[f.field for f in spec.filters],
        )

        # Generate workbook
        builder = TWBBuilder()
        builder.build_dashboard_from_csv(
            csv_path=csv_path,
            suggestion=dashboard,
            output_path=output_path,
        )

        # Apply branding if specified
        if spec.brand.primaryColor or spec.brand.fontFamily:
            editor = TWBEditor(output_path, clear_existing_content=False)
            if spec.brand.primaryColor:
                editor.apply_primary_color(spec.brand.primaryColor)
            if spec.brand.fontFamily:
                editor.apply_font_family(spec.brand.fontFamily)
            editor.save()

        # Read the generated file
        with open(output_path, "rb") as f:
            return f.read()

# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/health")
async def health():
    return {
        "status": "ok",
        "twilize_available": TWILIZE_AVAILABLE,
    }

@app.post("/generate")
async def generate(request: GenerateRequest):
    try:
        twbx_bytes = build_workbook(request.spec, request.sample_csv)
        twbx_base64 = base64.b64encode(twbx_bytes).decode("utf-8")
        return {"twbx_base64": twbx_base64}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
