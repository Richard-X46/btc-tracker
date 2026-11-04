"""House chart style: seaborn base, white ground, matplotlib grids, black spines.

Notebook:  from btc_tracker.dashboard.style import apply_house_style; apply_house_style()
App:       same call before building /api/figures/* payloads.
"""
import plotly.io as pio

HEIGHT = 650
WIDTH = 1000


def apply_house_style(paper: bool = False):
    pio.templates.default = "seaborn"
    t = pio.templates["seaborn"]
    t.layout.font.family = "Helvetica, Arial, sans-serif"
    t.layout.colorway = ["#0173B2", "#DE8F05", "#029E73", "#D55E00",
                         "#CC78BC", "#CA9161", "#FBAFE4", "#949494",
                         "#ECE133", "#56B4E9"]
    t.layout.paper_bgcolor = "white"
    t.layout.plot_bgcolor = "white"
    for ax in (t.layout.xaxis, t.layout.yaxis):
        ax.showgrid = True
        ax.gridcolor = "#cccccc"
        ax.ticks = "outside"
        ax.showline = True
        ax.linecolor = "black"
        if paper:
            ax.minor.showgrid = False
            ax.linewidth = 1
        else:
            ax.minor.showgrid = True
            ax.minor.gridcolor = "#eeeeee"
            ax.minor.griddash = "dot"
            ax.mirror = True
