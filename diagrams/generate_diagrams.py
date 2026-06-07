"""Generates the two required deliverable diagrams with matplotlib:
  - architecture.png  Medallion (Bronze/Silver/Gold) + S3 + Snowflake data flow
  - erd.png           Entity-relationship diagram for the Gold (ANALYTICS) layer

Run once: `python diagrams/generate_diagrams.py`. Pure rendering — no pipeline
dependencies — so regenerating never requires a live run or credentials.
"""
from __future__ import annotations

import os

import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch

OUT_DIR = os.path.dirname(os.path.abspath(__file__))

# Medallion layer color coding, used consistently across both diagrams.
BRONZE = "#CD7F32"
SILVER = "#9AA0A6"
GOLD = "#D4AF37"
QUARANTINE = "#B33A3A"
NEUTRAL = "#4A6FA5"
INK = "#222222"


def _box(ax, xy, w, h, text, facecolor, fontsize=10, fontweight="bold", textcolor="white"):
    x, y = xy
    patch = FancyBboxPatch(
        (x, y), w, h,
        boxstyle="round,pad=0.04,rounding_size=0.08",
        linewidth=1.4,
        edgecolor=INK,
        facecolor=facecolor,
    )
    ax.add_patch(patch)
    ax.text(x + w / 2, y + h / 2, text, ha="center", va="center",
            fontsize=fontsize, fontweight=fontweight, color=textcolor, linespacing=1.35)
    return (x + w / 2, y + h / 2, x, y, w, h)


def _arrow(ax, start_box, end_box, label="", color=INK, style="-|>", connectionstyle="arc3,rad=0.0", label_offset=(0, 0.12)):
    """Draw an arrow from the bottom-center of start_box to the top-center of end_box
    (boxes are the tuples returned by _box)."""
    sx, sy, sx0, sy0, sw, sh = start_box
    ex, ey, ex0, ey0, ew, eh = end_box
    start = (sx, sy0) if sy0 < sy else (sx, sy0 + sh)
    end = (ex, ey0 + eh) if (ey0 + eh) < sy else (ex, ey0)
    # Pick whichever vertical relationship makes sense (start above end, or below)
    if sy0 > ey0:
        start = (sx, sy0)
        end = (ex, ey0 + eh)
    else:
        start = (sx, sy0 + sh)
        end = (ex, ey0)
    ax.annotate(
        "", xy=end, xytext=start,
        arrowprops=dict(arrowstyle=style, color=color, lw=1.6, connectionstyle=connectionstyle),
    )
    if label:
        mx, my = (start[0] + end[0]) / 2 + label_offset[0], (start[1] + end[1]) / 2 + label_offset[1]
        ax.text(mx, my, label, ha="center", va="center", fontsize=8, color=color, style="italic")


# ---------------------------------------------------------------------------
def generate_architecture_diagram(path: str) -> None:
    fig, ax = plt.subplots(figsize=(13, 11))
    ax.set_xlim(0, 13)
    ax.set_ylim(0, 15.5)
    ax.axis("off")
    ax.set_title(
        "E-Commerce Web Log ETL — Medallion Architecture (S3 + Snowflake)",
        fontsize=15, fontweight="bold", color=INK, pad=14,
    )

    # ---- Sources ----------------------------------------------------------
    src_w, src_h = 2.6, 0.9
    sources = [
        _box(ax, (0.6, 13.6), src_w, src_h, "users.csv", NEUTRAL),
        _box(ax, (3.7, 13.6), src_w, src_h, "products.csv", NEUTRAL),
        _box(ax, (6.8, 13.6), src_w, src_h, "weblogs.csv\n(streamed in chunks)", NEUTRAL),
    ]
    ax.text(5.0, 14.85, "Phase 1 — Extract (pandas, chunksize)", ha="center", fontsize=10, style="italic", color=INK)

    # ---- Bronze ------------------------------------------------------------
    bronze = _box(
        ax, (0.6, 11.6), 8.8, 1.5,
        "BRONZE   (validate · dedupe · orphan-check · quarantine-and-continue)\n"
        "S3:        bronze/<source>/ingest_date=YYYY-MM-DD/*.parquet\n"
        "Snowflake RAW:  BRONZE_WEBLOGS · BRONZE_USERS · BRONZE_PRODUCTS",
        BRONZE, fontsize=8.7,
    )
    for s in sources:
        _arrow(ax, s, bronze)

    quarantine = _box(
        ax, (9.8, 11.75), 2.7, 1.2,
        "QUARANTINE\nrejected rows +\nrun metadata\n(source/date/run_id)",
        QUARANTINE, fontsize=8.5,
    )
    _arrow(ax, bronze, quarantine, label="invalid rows", color=QUARANTINE,
           connectionstyle="arc3,rad=-0.25")

    # ---- Silver ------------------------------------------------------------
    silver = _box(
        ax, (0.6, 9.3), 8.8, 1.5,
        "SILVER   (parse timestamps · categorize actions · sort · session metrics · enrich)\n"
        "S3:        silver/{weblogs_clean, users_clean, products_clean}/etl_run_date=.../*.parquet\n"
        "Snowflake STAGING:  WEBLOGS_CLEAN · USERS_CLEAN · PRODUCTS_CLEAN",
        SILVER, fontsize=8.7, textcolor=INK,
    )
    _arrow(ax, bronze, silver)
    ax.text(11.15, 11.2, "Silver RE-READS Bronze's\npersisted output via\nStorageBackend — each\nlayer stays independently\nre-runnable.",
            ha="center", va="center", fontsize=7.3, style="italic", color=INK)

    # ---- COPY INTO bridge ---------------------------------------------------
    copy_into = _box(
        ax, (2.4, 7.55), 5.2, 0.85,
        "COPY INTO  (named external stage, STORAGE INTEGRATION,\nFILE_FORMAT = PARQUET, MATCH_BY_COLUMN_NAME)",
        "#EDEDED", fontsize=8.5, textcolor=INK, fontweight="normal",
    )
    _arrow(ax, silver, copy_into, label="bronze_stage / silver_stage")

    # ---- Gold ---------------------------------------------------------------
    gold = _box(
        ax, (0.6, 5.2), 8.8, 1.85,
        "GOLD  (Snowflake ANALYTICS — derived in-warehouse, no S3 stage)\n"
        "MERGE INTO DIM_USER / DIM_PRODUCT  (idempotent surrogate-key upserts)\n"
        "INSERT … SELECT  ->  FACT_USER_ACTIVITY   (CLUSTER BY etl_run_date, action)\n"
        "INSERT … SELECT  ->  AGG_SESSION_METRICS  (CLUSTER BY etl_run_date)",
        GOLD, fontsize=9, textcolor=INK,
    )
    _arrow(ax, copy_into, gold, label="build_gold(): MERGE / INSERT…SELECT")

    # ---- Consumers -----------------------------------------------------------
    bi = _box(ax, (0.6, 3.0), 4.1, 1.1,
              "BI Analytics\n10 queries in sql/analytics/\n(most-viewed, conversion, cohorts, …)",
              NEUTRAL, fontsize=8.5)
    audit = _box(ax, (5.3, 3.0), 4.1, 1.1,
                 "ETL_AUDIT_LOG +\nPost-load validation checks\n(orphan FK / dup log_id / neg. duration)",
                 NEUTRAL, fontsize=8.5)
    _arrow(ax, gold, bi, connectionstyle="arc3,rad=0.15")
    _arrow(ax, gold, audit, connectionstyle="arc3,rad=-0.15")

    report = _box(ax, (3.0, 1.0), 4.6, 1.1,
                  "data_quality_report.md\n(rendered from RunMetrics — same\naccumulator that feeds ETL_AUDIT_LOG)",
                  "#EDEDED", fontsize=8.5, textcolor=INK, fontweight="normal")
    _arrow(ax, audit, report, connectionstyle="arc3,rad=0.0")
    _arrow(ax, bi, report, connectionstyle="arc3,rad=0.0")

    # legend
    legend_x = 9.9
    for i, (label, color) in enumerate([("Bronze", BRONZE), ("Silver", SILVER), ("Gold", GOLD), ("Quarantine", QUARANTINE)]):
        ly = 4.6 - i * 0.55
        ax.add_patch(FancyBboxPatch((legend_x, ly), 0.4, 0.32, boxstyle="round,pad=0.02", facecolor=color, edgecolor=INK))
        ax.text(legend_x + 0.55, ly + 0.16, label, va="center", fontsize=9, color=INK)

    fig.tight_layout()
    fig.savefig(path, dpi=160, bbox_inches="tight")
    plt.close(fig)


# ---------------------------------------------------------------------------
def generate_erd_diagram(path: str) -> None:
    fig, ax = plt.subplots(figsize=(13, 9))
    ax.set_xlim(0, 13)
    ax.set_ylim(0, 9.5)
    ax.axis("off")
    ax.set_title(
        "Gold Layer (ANALYTICS schema) — Entity-Relationship Diagram",
        fontsize=15, fontweight="bold", color=INK, pad=14,
    )

    def table(xy, w, h, name, columns, color):
        x, y = xy
        header_h = 0.55
        ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.03,rounding_size=0.05",
                                    linewidth=1.4, edgecolor=INK, facecolor="white"))
        ax.add_patch(FancyBboxPatch((x, y + h - header_h), w, header_h,
                                    boxstyle="round,pad=0.0,rounding_size=0.0",
                                    linewidth=1.2, edgecolor=INK, facecolor=color))
        ax.text(x + w / 2, y + h - header_h / 2, name, ha="center", va="center",
                fontsize=10.5, fontweight="bold", color="white" if color != GOLD else INK)
        for i, col in enumerate(columns):
            ax.text(x + 0.18, y + h - header_h - 0.32 - i * 0.32, col,
                    ha="left", va="center", fontsize=8.3, color=INK, family="monospace")
        return (x + w / 2, y + h / 2, x, y, w, h)

    dim_user = table(
        (0.6, 6.1), 3.4, 3.1, "DIM_USER", [
            "PK user_sk        NUMBER (autoincrement)",
            "   user_id        NUMBER",
            "   user_name      VARCHAR(255)",
            "   email          VARCHAR(255)",
            "   signup_date    DATE",
            "   dw_created_at  TIMESTAMP_NTZ",
            "   dw_updated_at  TIMESTAMP_NTZ",
        ], BRONZE,
    )

    dim_product = table(
        (9.0, 6.1), 3.4, 2.7, "DIM_PRODUCT", [
            "PK product_sk    NUMBER (autoincrement)",
            "   product_id    NUMBER",
            "   product_name  VARCHAR(255)",
            "   category      VARCHAR(100)",
            "   price         FLOAT",
            "   dw_created_at TIMESTAMP_NTZ",
        ], BRONZE,
    )

    fact = table(
        (4.6, 3.0), 3.9, 3.55, "FACT_USER_ACTIVITY\n(CLUSTER BY etl_run_date, action)", [
            "PK activity_id   NUMBER (autoincrement)",
            "   log_id        NUMBER",
            "FK user_sk       -> DIM_USER",
            "FK product_sk    -> DIM_PRODUCT",
            "   session_id    VARCHAR(100)",
            "   action        VARCHAR(50)",
            "   action_ts     TIMESTAMP_NTZ",
            "   etl_run_id    VARCHAR(36)",
            "   etl_run_date  DATE",
        ], GOLD,
    )
    # tighten the multi-line header by re-drawing title smaller — acceptable visual trade-off
    ax.texts[-9].set_fontsize(8.5)

    agg = table(
        (0.6, 0.5), 3.7, 3.85, "AGG_SESSION_METRICS\n(CLUSTER BY etl_run_date)", [
            "PK session_id        VARCHAR(100)",
            "FK user_sk           -> DIM_USER",
            "   session_start     TIMESTAMP_NTZ",
            "   session_end       TIMESTAMP_NTZ",
            "   session_duration_s FLOAT",
            "   total_actions     NUMBER",
            "   total_views       NUMBER",
            "   total_cart_adds   NUMBER",
            "   total_purchases   NUMBER",
            "   conversion_rate   FLOAT",
            "   is_abandoned_cart BOOLEAN",
            "   is_high_activity  BOOLEAN",
            "   etl_run_date      DATE",
        ], GOLD,
    )
    ax.texts[-13].set_fontsize(8.5)

    audit = table(
        (8.7, 0.5), 3.9, 2.35, "ETL_AUDIT_LOG\n(no FK — one row per run x layer)", [
            "PK audit_id       NUMBER (autoincrement)",
            "   etl_run_id     VARCHAR(36)",
            "   layer          VARCHAR(20)",
            "   rows_extracted/quarantined/loaded",
            "   status         VARCHAR(20)",
            "   created_at     TIMESTAMP_NTZ",
        ], SILVER,
    )
    ax.texts[-6].set_fontsize(8.5)

    # Relationships — drawn as straight lines through the empty gaps BETWEEN boxes
    # (edge-midpoint to edge-midpoint) so they never cross over a table's column list.
    def relate(p1, p2, label):
        ax.annotate("", xy=p2, xytext=p1, arrowprops=dict(arrowstyle="-", color=INK, lw=1.3))
        mx, my = (p1[0] + p2[0]) / 2, (p1[1] + p2[1]) / 2
        ax.text(mx, my, label, ha="center", va="center", fontsize=7.5, style="italic",
                color=INK, bbox=dict(boxstyle="round,pad=0.12", facecolor="white", edgecolor="none", alpha=0.85))

    relate((4.6, 4.775), (4.0, 7.65), "user_sk")        # FACT.left  <-> DIM_USER.right    (gap: x in [4.0,4.6])
    relate((8.5, 4.775), (9.0, 7.45), "product_sk")     # FACT.right <-> DIM_PRODUCT.left  (gap: x in [8.5,9.0])
    relate((2.45, 4.35), (2.3, 6.1), "user_sk")          # AGG.top    <-> DIM_USER.bottom   (gap: y in [4.35,6.1])

    ax.text(6.55, 8.9, "Dimensions (Bronze color = slowly-changing reference data, refreshed via idempotent MERGE)",
            ha="center", fontsize=8.5, style="italic", color=INK)
    ax.text(6.55, 0.15, "Fact / Aggregate (Gold) tables are append-only, idempotent INSERT…SELECT guarded by NOT EXISTS",
            ha="center", fontsize=8.5, style="italic", color=INK)

    fig.tight_layout()
    fig.savefig(path, dpi=160, bbox_inches="tight")
    plt.close(fig)


def main():
    arch_path = os.path.join(OUT_DIR, "architecture.png")
    erd_path = os.path.join(OUT_DIR, "erd.png")
    generate_architecture_diagram(arch_path)
    generate_erd_diagram(erd_path)
    print(f"wrote {arch_path}")
    print(f"wrote {erd_path}")


if __name__ == "__main__":
    main()
