"""Draw the README charts from results/*.csv.  python scripts/charts.py"""
from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
RESULTS, DOCS = ROOT / "results", ROOT / "docs"
RELEASE = (RESULTS / "release.txt").read_text(encoding="utf-8").strip()
BLUE, ORANGE = "#2a78d6", "#eb6834"
INK, MUTED, GRID, SURFACE = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "axes.edgecolor": GRID,
    "axes.labelcolor": MUTED, "xtick.color": MUTED, "ytick.color": MUTED, "text.color": INK,
    "font.size": 10, "axes.spines.top": False, "axes.spines.right": False,
    "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8, "axes.axisbelow": True,
})


def survival_chart() -> None:
    c = pd.read_csv(RESULTS / "mart_cohort_survival.csv")
    c = c[c["entity_kind"] == "Company"]
    c["group"] = c["industry"].where(c["industry"] == "Food and beverage", "All other industries")
    g = c.groupby(["group", "age_years"])[["still_live", "registered"]].sum().reset_index()
    g["share"] = g["still_live"] / g["registered"]

    fig, ax = plt.subplots(figsize=(8, 4.2))
    for name, colour in [("All other industries", BLUE), ("Food and beverage", ORANGE)]:
        s = g[g["group"] == name].sort_values("age_years")
        ax.plot(s["age_years"], s["share"], color=colour, linewidth=2, marker="o", markersize=4)
        last = s.iloc[-1]
        ax.annotate(f"{name}", (last["age_years"], last["share"]), xytext=(6, 0),
                    textcoords="offset points", va="center", color=INK, fontsize=9)
    old = g[g["age_years"].between(15, 20)].groupby("group")[["still_live", "registered"]].sum()
    old_share = old["still_live"] / old["registered"]
    ax.annotate("Level for the first decade. Companies 15 to 20 years old:\n"
                f"{old_share['Food and beverage']:.0%} of F&B still live, vs "
                f"{old_share['All other industries']:.0%} elsewhere",
                (17, 0.23), xytext=(7.5, 0.80), textcoords="data",
                fontsize=9, color=MUTED, arrowprops={"arrowstyle": "-", "color": MUTED, "lw": 0.8})
    ax.set_ylim(0, 1)
    ax.set_xlim(0.5, g["age_years"].max() + 5)
    ax.yaxis.set_major_formatter(lambda v, _p: f"{v:.0%}")
    ax.set_xlabel("Years since registration")
    ax.set_ylabel("Still live today")
    ax.set_title("Singapore companies still live, by age", loc="left", fontsize=12, color=INK)
    fig.text(0.01, 0.01, "Companies registered 2005–2025. Source: ACRA register via data.gov.sg, "
             f"release {RELEASE}.", fontsize=8, color=MUTED)
    fig.tight_layout(rect=(0, 0.03, 1, 1))
    fig.savefig(DOCS / "survival_by_age.png", dpi=150)
    plt.close(fig)


def formations_chart() -> None:
    f = pd.read_csv(RESULTS / "mart_formations_monthly.csv", parse_dates=["month"])
    f = f[f["entity_kind"].isin(["Company", "Sole proprietor or partnership"])]
    y = f.groupby(f["month"].dt.year)["registrations"].sum()
    y = y[(y.index >= 2005) & (y.index <= 2025)]

    fig, ax = plt.subplots(figsize=(8, 3.8))
    ax.bar(y.index, y.values, color=BLUE, width=0.8)
    ax.grid(axis="x", visible=False)
    for year in (2014, 2020, 2025):
        ax.annotate(f"{y[year] / 1000:.1f}k", (year, y[year]), xytext=(0, 3), textcoords="offset points",
                    ha="center", fontsize=8, color=INK)
    ax.yaxis.set_major_formatter(lambda v, _p: f"{v / 1000:.0f}k")
    ax.set_title("New businesses registered per year (companies and sole proprietors)",
                 loc="left", fontsize=12, color=INK)
    fig.text(0.01, 0.01, f"Source: ACRA register via data.gov.sg, release {RELEASE}.", fontsize=8, color=MUTED)
    fig.tight_layout(rect=(0, 0.03, 1, 1))
    fig.savefig(DOCS / "registrations_per_year.png", dpi=150)
    plt.close(fig)


if __name__ == "__main__":
    DOCS.mkdir(exist_ok=True)
    survival_chart()
    formations_chart()
