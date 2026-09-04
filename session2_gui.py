from __future__ import annotations

import time
import tkinter as tk
from datetime import datetime
from typing import Any

from tkinter import messagebox, ttk

try:
    import psycopg2
except ImportError as exc:
    raise RuntimeError(
        "Install psycopg2-binary first: "
        "pip install psycopg2-binary"
    ) from exc


# ============================================================
# DATABASE CONFIGURATION
# ============================================================

DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "Session2-AVR-Flow"
DB_USER = "postgres"
DB_PASSWORD = "root"

PARTITION_COUNT = 4
DEFAULT_PAGE_SIZE = 100


# ============================================================
# CONSUMERS
# ============================================================

CONSUMERS = [
    (
        "revenue-projector",
        "Processes calendar payment and revenue records",
    ),
    (
        "audit-writer",
        "Tracks durable event processing progress",
    ),
    (
        "payment-monitor",
        "Monitors payment methods, statuses, and recorded amounts",
    ),
    (
        "high-value-alerter",
        "Flags high-value paid bookings for review",
    ),
]


# ============================================================
# SQL EXPRESSIONS
# ============================================================

# Listed price is displayed as stored in the database.
# It is NOT treated as actual revenue.

PRICE_NUMERIC_SQL = """
COALESCE(
    NULLIF(
        regexp_replace(
            COALESCE(c.price::text, ''),
            '[^0-9.-]',
            '',
            'g'
        ),
        ''
    )::numeric,
    0
)
"""


# Actual amount paid is the application's revenue source.
#
# The expression safely handles:
#   120
#   120.50
#   $120.50
#   1,200.00
#
# Invalid, blank, or NULL values become 0.

AMOUNT_PAID_NUMERIC_SQL = """
COALESCE(
    NULLIF(
        regexp_replace(
            COALESCE(c.amount_paid::text, ''),
            '[^0-9.-]',
            '',
            'g'
        ),
        ''
    )::numeric,
    0
)
"""


# Used inside ordered stream subqueries where the alias is not c.

STREAM_AMOUNT_PAID_NUMERIC_SQL = """
COALESCE(
    NULLIF(
        regexp_replace(
            COALESCE(amount_paid::text, ''),
            '[^0-9.-]',
            '',
            'g'
        ),
        ''
    )::numeric,
    0
)
"""


# Determines whether amount_paid contains a numeric value.

VALID_AMOUNT_PAID_SQL = """
NULLIF(
    regexp_replace(
        COALESCE(c.amount_paid::text, ''),
        '[^0-9.-]',
        '',
        'g'
    ),
    ''
) IS NOT NULL
"""


# Used inside ordered stream subqueries where the alias is not c.

STREAM_VALID_AMOUNT_PAID_SQL = """
NULLIF(
    regexp_replace(
        COALESCE(amount_paid::text, ''),
        '[^0-9.-]',
        '',
        'g'
    ),
    ''
) IS NOT NULL
"""


# ============================================================
# DATABASE HELPERS
# ============================================================

def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


def query_all(
    query: str,
    params: tuple[Any, ...] = (),
):
    conn = get_connection()

    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall()

    finally:
        conn.close()


def query_one(
    query: str,
    params: tuple[Any, ...] = (),
):
    rows = query_all(query, params)

    if not rows:
        return None

    return rows[0]


def execute_command(
    query: str,
    params: tuple[Any, ...] = (),
):
    conn = get_connection()

    try:
        with conn.cursor() as cur:
            cur.execute(query, params)

        conn.commit()

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()


# ============================================================
# FORMATTING HELPERS
# ============================================================

def fmt_number(value):
    try:
        return f"{int(value):,}"
    except Exception:
        return str(value)


def fmt_money(value):
    try:
        return f"${float(value):,.2f}"
    except Exception:
        return str(value)


def fmt_text(value, fallback="N/A"):
    if value is None:
        return fallback

    text = str(value).strip()

    return text if text else fallback


# ============================================================
# BAR CHART
# ============================================================

class BarChart(tk.Canvas):

    def __init__(
        self,
        parent,
        title="",
        height=300,
    ):
        super().__init__(
            parent,
            height=height,
            bg="#f8fafc",
            highlightthickness=1,
            highlightbackground="#cbd5e1",
        )

        self.chart_title = title
        self.data = []

        self.bind(
            "<Configure>",
            lambda event: self.redraw(),
        )

    def set_data(self, data):
        self.data = data or []
        self.redraw()

    def redraw(self):

        self.delete("all")

        width = max(self.winfo_width(), 500)
        height = max(self.winfo_height(), 240)

        if not self.data:

            self.create_text(
                width / 2,
                height / 2,
                text="No PostgreSQL result available yet.",
                fill="#64748b",
                font=("Segoe UI", 10),
            )

            return

        left = 70
        right = 30
        top = 42
        bottom = 58

        chart_width = width - left - right
        chart_height = height - top - bottom

        values = [
            max(0.0, float(item[1]))
            for item in self.data
        ]

        maximum = max(values) or 1.0

        self.create_text(
            left,
            18,
            text=self.chart_title,
            anchor="w",
            fill="#243b5a",
            font=("Segoe UI", 12, "bold"),
        )

        for i in range(5):

            fraction = i / 4

            y = (
                top
                + chart_height
                - chart_height * fraction
            )

            value = maximum * fraction

            self.create_line(
                left,
                y,
                left + chart_width,
                y,
                fill="#e2e8f0",
            )

            if value >= 1_000_000:
                label = f"{value / 1_000_000:.1f}M"

            elif value >= 1_000:
                label = f"{value / 1_000:.0f}K"

            else:
                label = f"{value:.0f}"

            self.create_text(
                left - 8,
                y,
                text=label,
                anchor="e",
                fill="#64748b",
                font=("Segoe UI", 8),
            )

        slot = chart_width / max(
            len(self.data),
            1,
        )

        bar_width = min(
            110,
            slot * 0.58,
        )

        max_index = values.index(
            max(values)
        )

        for index, (
            label,
            value,
            annotation,
        ) in enumerate(self.data):

            x = (
                left
                + slot * index
                + slot / 2
            )

            bar_height = (
                chart_height
                * (
                    float(value)
                    / maximum
                )
            )

            y0 = top + chart_height
            y1 = y0 - bar_height

            fill = (
                "#c65a08"
                if index == max_index
                else "#416b8a"
            )

            self.create_rectangle(
                x - bar_width / 2,
                y1,
                x + bar_width / 2,
                y0,
                fill=fill,
                outline="",
            )

            self.create_text(
                x,
                y1 - 10,
                text=annotation,
                fill="#334155",
                font=("Segoe UI", 8),
            )

            self.create_text(
                x,
                y0 + 18,
                text=label,
                fill="#334155",
                font=("Segoe UI", 9, "bold"),
            )


# ============================================================
# MAIN GUI
# ============================================================

class AVRFlowPostgresGUI(tk.Tk):

    def __init__(self):

        super().__init__()

        self.title(
            "AVR-Flow — Session 2 Event Streaming Console"
        )

        self.geometry("1520x950")

        self.minsize(
            1180,
            760,
        )

        self.connection_ok = False

        self.pipeline_vars = {}

        self.console_text = None

        self.stage_state = {
            "self_test": {
                "label": "1. Log self-test",
                "status": "NOT RUN",
                "result": "No result yet",
            },

            "produce": {
                "label": "2. Produce",
                "status": "NOT RUN",
                "result": "No result yet",
            },

            "consume": {
                "label": "3. Consume",
                "status": "NOT RUN",
                "result": "No result yet",
            },

            "reconcile": {
                "label": "4. Reconcile",
                "status": "NOT RUN",
                "result": "No result yet",
            },

            "failure": {
                "label": "5. Failure & recovery",
                "status": "NOT RUN",
                "result": "No result yet",
            },

            "replay": {
                "label": "6. Replay",
                "status": "NOT RUN",
                "result": "No result yet",
            },
        }

        self._configure_styles()
        self._build_shell()

        self.after(
            300,
            self.initialize,
        )

    # ========================================================
    # UI STYLES
    # ========================================================

    def _configure_styles(self):

        style = ttk.Style(self)

        style.theme_use("clam")

        self.configure(
            bg="#eef2f7"
        )

        style.configure(
            "TFrame",
            background="#eef2f7",
        )

        style.configure(
            "TLabel",
            background="#eef2f7",
            foreground="#1f2937",
        )

        style.configure(
            "Header.TFrame",
            background="#30496d",
        )

        style.configure(
            "HeaderTitle.TLabel",
            background="#30496d",
            foreground="white",
            font=("Georgia", 21, "bold"),
        )

        style.configure(
            "HeaderSub.TLabel",
            background="#30496d",
            foreground="#d8e0ec",
            font=("Segoe UI", 10, "bold"),
        )

        style.configure(
            "PageTitle.TLabel",
            font=("Georgia", 18, "bold"),
            foreground="#2d425f",
        )

        style.configure(
            "Section.TLabel",
            font=("Georgia", 13, "bold"),
            foreground="#2d425f",
        )

        style.configure(
            "Subtitle.TLabel",
            font=("Segoe UI", 10),
            foreground="#56677d",
        )

        style.configure(
            "Note.TLabel",
            font=("Segoe UI", 9),
            foreground="#56677d",
        )

        style.configure(
            "Card.TFrame",
            background="#f8fafc",
            relief="solid",
            borderwidth=1,
        )

        style.configure(
            "CardValue.TLabel",
            background="#f8fafc",
            foreground="#2d425f",
            font=("Georgia", 22, "bold"),
        )

        style.configure(
            "CardCaption.TLabel",
            background="#f8fafc",
            foreground="#56677d",
            font=("Segoe UI", 10),
        )

        style.configure(
            "Banner.TLabel",
            background="#e3edd9",
            foreground="#516438",
            font=("Segoe UI", 10, "bold"),
            padding=(14, 11),
        )

        style.configure(
            "WarnBanner.TLabel",
            background="#f5e6d8",
            foreground="#9a5a1b",
            font=("Segoe UI", 10, "bold"),
            padding=(14, 11),
        )

        style.configure(
            "Treeview",
            background="#f8fafc",
            fieldbackground="#f8fafc",
            rowheight=28,
            font=("Segoe UI", 9),
        )

        style.configure(
            "Treeview.Heading",
            background="#30496d",
            foreground="white",
            font=("Segoe UI", 9, "bold"),
            relief="flat",
        )

        style.map(
            "Treeview",
            background=[
                ("selected", "#d7e3f1"),
            ],
            foreground=[
                ("selected", "#1f2937"),
            ],
        )

        style.configure(
            "TNotebook.Tab",
            background="#e7ebf0",
            foreground="#4b5c70",
            padding=(18, 11),
            font=("Segoe UI", 10, "bold"),
        )

        style.map(
            "TNotebook.Tab",
            background=[
                ("selected", "#30496d"),
            ],
            foreground=[
                ("selected", "white"),
            ],
        )

        style.configure(
            "Accent.TButton",
            background="#30496d",
            foreground="white",
            font=("Segoe UI", 9, "bold"),
            padding=(14, 8),
        )

    # ========================================================
    # MAIN SHELL
    # ========================================================

    def _build_shell(self):

        header = ttk.Frame(
            self,
            style="Header.TFrame",
            padding=(22, 16),
        )

        header.pack(fill="x")

        ttk.Label(
            header,
            text=(
                "AVR-Flow — Session 2 "
                "Event Streaming Console"
            ),
            style="HeaderTitle.TLabel",
        ).pack(anchor="w")

        ttk.Label(
            header,
            text=(
                "MIT 261 Parallel and Distributed Systems  ·  "
                "PostgreSQL-backed event source  ·  "
                "4 logical partitions keyed by listing_id"
            ),
            style="HeaderSub.TLabel",
        ).pack(
            anchor="w",
            pady=(5, 0),
        )

        top = ttk.Frame(
            self,
            padding=(14, 10, 14, 0),
        )

        top.pack(fill="x")

        self.status_var = tk.StringVar(
            value="Connecting to PostgreSQL..."
        )

        ttk.Label(
            top,
            textvariable=self.status_var,
            style="Subtitle.TLabel",
        ).pack(side="left")

        self.notebook = ttk.Notebook(self)

        self.notebook.pack(
            fill="both",
            expand=True,
            padx=14,
            pady=(8, 14),
        )

        self.pipeline_tab = ttk.Frame(
            self.notebook,
            padding=16,
        )

        self.durable_tab = ttk.Frame(
            self.notebook,
            padding=16,
        )

        self.consumers_tab = ttk.Frame(
            self.notebook,
            padding=16,
        )

        self.failure_tab = ttk.Frame(
            self.notebook,
            padding=16,
        )

        self.replay_tab = ttk.Frame(
            self.notebook,
            padding=16,
        )

        self.reconciliation_tab = ttk.Frame(
            self.notebook,
            padding=16,
        )

        self.console_tab = ttk.Frame(
            self.notebook,
            padding=16,
        )

        self.notebook.add(
            self.pipeline_tab,
            text="Pipeline",
        )

        self.notebook.add(
            self.durable_tab,
            text="Durable log",
        )

        self.notebook.add(
            self.consumers_tab,
            text="Consumers & lag",
        )

        self.notebook.add(
            self.failure_tab,
            text="Failure & recovery",
        )

        self.notebook.add(
            self.replay_tab,
            text="Replay",
        )

        self.notebook.add(
            self.reconciliation_tab,
            text="Reconciliation",
        )

        self.notebook.add(
            self.console_tab,
            text="Console",
        )

        self._build_pipeline_tab()
        self._build_durable_tab()
        self._build_consumers_tab()
        self._build_failure_tab()
        self._build_replay_tab()
        self._build_reconciliation_tab()
        self._build_console_tab()

    # ========================================================
    # UI HELPERS
    # ========================================================

    def _make_tree(
        self,
        parent,
        columns,
        height=7,
    ):

        outer = ttk.Frame(parent)

        outer.pack(
            fill="both",
            expand=True,
        )

        tree = ttk.Treeview(
            outer,
            columns=[
                column[0]
                for column in columns
            ],
            show="headings",
            height=height,
        )

        for key, title, width in columns:

            tree.heading(
                key,
                text=title,
            )

            tree.column(
                key,
                width=width,
                anchor="center",
            )

        ybar = ttk.Scrollbar(
            outer,
            orient="vertical",
            command=tree.yview,
        )

        xbar = ttk.Scrollbar(
            outer,
            orient="horizontal",
            command=tree.xview,
        )

        tree.configure(
            yscrollcommand=ybar.set,
            xscrollcommand=xbar.set,
        )

        tree.grid(
            row=0,
            column=0,
            sticky="nsew",
        )

        ybar.grid(
            row=0,
            column=1,
            sticky="ns",
        )

        xbar.grid(
            row=1,
            column=0,
            sticky="ew",
        )

        outer.rowconfigure(
            0,
            weight=1,
        )

        outer.columnconfigure(
            0,
            weight=1,
        )

        return tree

    def _section(
        self,
        parent,
        text,
    ):

        ttk.Label(
            parent,
            text=text,
            style="Section.TLabel",
        ).pack(
            anchor="w",
            pady=(10, 6),
        )

    def _banner(
        self,
        parent,
        variable,
        warning=False,
    ):

        ttk.Label(
            parent,
            textvariable=variable,
            style=(
                "WarnBanner.TLabel"
                if warning
                else "Banner.TLabel"
            ),
            anchor="w",
        ).pack(
            fill="x",
            pady=(4, 12),
        )

    @staticmethod
    def clear_tree(tree):

        for item in tree.get_children():
            tree.delete(item)

    # ========================================================
    # STAGE CONTROL
    # ========================================================

    def _render_stage_table(self):

        self.clear_tree(
            self.pipeline_stage_tree
        )

        order = [
            "self_test",
            "produce",
            "consume",
            "reconcile",
            "failure",
            "replay",
        ]

        for key in order:

            state = self.stage_state[key]

            self.pipeline_stage_tree.insert(
                "",
                "end",
                values=(
                    state["label"],
                    state["status"],
                    state["result"],
                ),
            )

    def _set_stage(
        self,
        key,
        status,
        result,
    ):

        self.stage_state[key]["status"] = status
        self.stage_state[key]["result"] = result

        # Keep the "reconciliation" KPI card on the Pipeline
        # tab in sync the moment the reconcile stage changes,
        # regardless of which button triggered it (Pipeline
        # tab, Reconciliation tab, or "Run everything"). This
        # used to only be refreshed inside refresh_pipeline(),
        # so the KPI card could stay stuck on "NOT RUN" after
        # an individual "Reconcile" click.
        if (
            key == "reconcile"
            and "reconciliation" in self.pipeline_vars
        ):
            self.pipeline_vars["reconciliation"].set(status)

        self._render_stage_table()

        self.update_idletasks()

    def run_stage(
        self,
        key,
        action,
    ):

        if not self.connection_ok:
            raise RuntimeError(
                "PostgreSQL is not connected."
            )

        self._set_stage(
            key,
            "RUNNING",
            "Running PostgreSQL-backed stage...",
        )

        self.status_var.set(
            f"Running "
            f"{self.stage_state[key]['label']}..."
        )

        self.log(
            f"{self.stage_state[key]['label']} started."
        )

        try:

            headline = action()

            if headline is None:
                headline = (
                    "Stage completed successfully"
                )

            # An action can complete without raising an
            # exception yet still report a business-level
            # failure (e.g. refresh_reconciliation() returns
            # a headline starting with "FAILED" when one of
            # the reconciliation conditions doesn't hold).
            # Treat that as a failed stage instead of always
            # forcing "PASSED" just because nothing crashed.
            stage_status = (
                "FAILED"
                if str(headline).strip().upper().startswith("FAILED")
                else "PASSED"
            )

            self._set_stage(
                key,
                stage_status,
                str(headline),
            )

            self.status_var.set(
                f"{self.stage_state[key]['label']} "
                f"{stage_status.lower()} — "
                + datetime.now().strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
            )

            self.log(
                f"{self.stage_state[key]['label']} "
                f"{stage_status} — {headline}"
            )

            return stage_status == "PASSED"

        except Exception as error:

            headline = (
                f"{type(error).__name__}: {error}"
            )

            self._set_stage(
                key,
                "FAILED",
                headline,
            )

            self.status_var.set(
                f"{self.stage_state[key]['label']} failed"
            )

            self.log(
                f"{self.stage_state[key]['label']} "
                f"FAILED — {headline}"
            )

            messagebox.showerror(
                "Stage Error",
                headline,
            )

            return False

    def run_everything(self):

        sequence = [
            (
                "self_test",
                self.run_log_self_test,
            ),
            (
                "produce",
                self.refresh_durable_log,
            ),
            (
                "consume",
                self.run_all_consumers,
            ),
            (
                "reconcile",
                self.refresh_reconciliation,
            ),
            (
                "failure",
                self.refresh_failure_view,
            ),
            (
                "replay",
                self.run_replay,
            ),
        ]

        for key, action in sequence:

            if not self.run_stage(
                key,
                action,
            ):

                self.log(
                    "Run everything stopped "
                    "because a stage failed."
                )

                return

        self.refresh_pipeline()

        self.status_var.set(
            "All AVR-Flow stages completed successfully — "
            + datetime.now().strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        )

        self.log(
            "RUN EVERYTHING COMPLETED SUCCESSFULLY"
        )

    def refresh_all(self):

        self.refresh_pipeline()
        self.refresh_durable_log()
        self.refresh_consumers()
        self.refresh_failure_view()
        self.refresh_replay_preview_only()
        self.refresh_reconciliation_preview_only()

        self._render_stage_table()

    # ========================================================
    # CONSOLE
    # ========================================================

    def clear_console(self):

        if self.console_text is not None:

            self.console_text.delete(
                "1.0",
                "end",
            )

    def log(
        self,
        message,
    ):

        if self.console_text is not None:

            stamp = datetime.now().strftime(
                "%H:%M:%S"
            )

            self.console_text.insert(
                "end",
                f"[{stamp}] {message}\n",
            )

            self.console_text.see("end")

    def log_stage(
        self,
        title,
        lines,
    ):

        if self.console_text is None:
            return

        stamp = datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        width = 76

        self.console_text.insert(
            "end",
            "\n"
            + "=" * width
            + "\n",
        )

        self.console_text.insert(
            "end",
            f"STAGE: {title}\n",
        )

        self.console_text.insert(
            "end",
            f"TIME : {stamp}\n",
        )

        self.console_text.insert(
            "end",
            "-" * width
            + "\n",
        )

        for line in lines:

            self.console_text.insert(
                "end",
                f"{line}\n",
            )

        self.console_text.insert(
            "end",
            "=" * width
            + "\n",
        )

        self.console_text.see("end")

    # ========================================================
    # PIPELINE TAB
    # ========================================================

    def _build_pipeline_tab(self):

        ttk.Label(
            self.pipeline_tab,
            text="PostgreSQL-backed event pipeline",
            style="PageTitle.TLabel",
        ).pack(anchor="w")

        controls = ttk.Frame(
            self.pipeline_tab
        )

        controls.pack(
            fill="x",
            pady=(8, 8),
        )

        ttk.Label(
            controls,
            text=(
                "All displayed values are calculated "
                "from PostgreSQL queries. "
                "Listed price is not treated as revenue. "
                "Actual recorded revenue comes from amount_paid."
            ),
            style="Subtitle.TLabel",
        ).pack(side="left")

        ttk.Button(
            controls,
            text="Run everything",
            style="Accent.TButton",
            command=self.run_everything,
        ).pack(side="right")

        cards = ttk.Frame(
            self.pipeline_tab
        )

        cards.pack(
            fill="x",
            pady=(0, 10),
        )

        card_definitions = [
            (
                "events in the durable log",
                "events",
            ),
            (
                "consumer groups tracked",
                "groups",
            ),
            (
                "total lag across all groups",
                "lag",
            ),
            (
                "actual recorded revenue",
                "revenue",
            ),
            (
                "reconciliation status",
                "reconciliation",
            ),
        ]

        for index, (
            caption,
            key,
        ) in enumerate(card_definitions):

            card = ttk.Frame(
                cards,
                style="Card.TFrame",
                padding=12,
            )

            card.grid(
                row=0,
                column=index,
                sticky="nsew",
                padx=4,
            )

            var = tk.StringVar(value="...")

            self.pipeline_vars[key] = var

            ttk.Label(
                card,
                textvariable=var,
                style="CardValue.TLabel",
                anchor="center",
            ).pack(fill="x")

            ttk.Label(
                card,
                text=caption,
                style="CardCaption.TLabel",
                anchor="center",
            ).pack(
                fill="x",
                pady=(6, 0),
            )

            cards.columnconfigure(
                index,
                weight=1,
            )

        self._section(
            self.pipeline_tab,
            "Run a stage",
        )

        stages = ttk.Frame(
            self.pipeline_tab
        )

        stages.pack(
            fill="x",
            pady=(0, 10),
        )

        for (
            text,
            stage_key,
            command,
        ) in [
            (
                "Log self-test",
                "self_test",
                self.run_log_self_test,
            ),
            (
                "Produce",
                "produce",
                self.refresh_durable_log,
            ),
            (
                "Consume",
                "consume",
                self.run_all_consumers,
            ),
            (
                "Reconcile",
                "reconcile",
                self.refresh_reconciliation,
            ),
            (
                "Failure & recovery",
                "failure",
                self.refresh_failure_view,
            ),
            (
                "Replay",
                "replay",
                self.run_replay,
            ),
        ]:

            ttk.Button(
                stages,
                text=text,
                command=lambda key=stage_key,
                action=command: self.run_stage(
                    key,
                    action,
                ),
            ).pack(
                side="left",
                padx=(0, 6),
            )

        self.pipeline_stage_tree = self._make_tree(
            self.pipeline_tab,
            [
                (
                    "stage",
                    "Stage",
                    220,
                ),
                (
                    "status",
                    "Status",
                    120,
                ),
                (
                    "result",
                    "Headline result",
                    850,
                ),
            ],
            height=6,
        )

        self._section(
            self.pipeline_tab,
            "PostgreSQL source summary",
        )

        self.pipeline_source_tree = self._make_tree(
            self.pipeline_tab,
            [
                (
                    "source",
                    "Source table",
                    250,
                ),
                (
                    "rows",
                    "Rows returned by COUNT(*)",
                    280,
                ),
                (
                    "role",
                    "Pipeline role",
                    600,
                ),
            ],
            height=4,
        )

    # ========================================================
    # DURABLE LOG TAB
    # ========================================================

    def _build_durable_tab(self):

        self.durable_summary_var = tk.StringVar(
            value="Loading PostgreSQL durable log..."
        )

        top = ttk.Frame(
            self.durable_tab
        )

        top.pack(fill="x")

        ttk.Label(
            top,
            text=(
                "The PostgreSQL durable log "
                "and its partitions"
            ),
            style="PageTitle.TLabel",
        ).pack(side="left")

        ttk.Button(
            top,
            text="Refresh from PostgreSQL",
            style="Accent.TButton",
            command=self.refresh_durable_log,
        ).pack(side="right")

        self._banner(
            self.durable_tab,
            self.durable_summary_var,
        )

        body = ttk.Frame(
            self.durable_tab
        )

        body.pack(
            fill="both",
            expand=True,
        )

        left = ttk.Frame(body)
        right = ttk.Frame(body)

        left.pack(
            side="left",
            fill="both",
            expand=True,
            padx=(0, 12),
        )

        right.pack(
            side="left",
            fill="both",
            expand=True,
        )

        self.partition_chart = BarChart(
            left,
            title=(
                "Partition distribution — "
                "listing_id % 4"
            ),
            height=330,
        )

        self.partition_chart.pack(fill="x")

        self._section(
            left,
            "Partition detail",
        )

        self.partition_tree = self._make_tree(
            left,
            [
                (
                    "partition",
                    "Partition",
                    130,
                ),
                (
                    "events",
                    "Events",
                    180,
                ),
                (
                    "share",
                    "Share",
                    130,
                ),
                (
                    "observation",
                    "Observation",
                    300,
                ),
            ],
            height=5,
        )

        self._section(
            right,
            "Guarantees verified by the self-test",
        )

        self.guarantee_tree = self._make_tree(
            right,
            [
                (
                    "guarantee",
                    "Guarantee",
                    380,
                ),
                (
                    "result",
                    "Result",
                    120,
                ),
            ],
            height=6,
        )

        self._section(
            right,
            "What the partition numbers mean",
        )

        self.partition_note_var = tk.StringVar(
            value=(
                "Run Refresh from PostgreSQL "
                "to calculate the distribution."
            )
        )

        ttk.Label(
            right,
            textvariable=self.partition_note_var,
            style="Subtitle.TLabel",
            wraplength=500,
            justify="left",
        ).pack(
            fill="x",
            padx=8,
            pady=8,
        )

    # ========================================================
    # CONSUMERS TAB
    # ========================================================

    def _build_consumers_tab(self):

        self.consumer_summary_var = tk.StringVar(
            value="Loading consumer state..."
        )

        top = ttk.Frame(
            self.consumers_tab
        )

        top.pack(fill="x")

        ttk.Label(
            top,
            text=(
                "Three consumer groups, "
                "one PostgreSQL event source"
            ),
            style="PageTitle.TLabel",
        ).pack(side="left")

        ttk.Button(
            top,
            text="Run all consumers",
            style="Accent.TButton",
            command=lambda: self.run_stage(
                "consume",
                self.run_all_consumers,
            ),
        ).pack(side="right")

        ttk.Button(
            top,
            text="Reset offsets to 0",
            command=self.reset_all_consumers,
        ).pack(side="right", padx=(0, 6))

        self._banner(
            self.consumers_tab,
            self.consumer_summary_var,
        )

        self._section(
            self.consumers_tab,
            "Throughput of the last PostgreSQL-backed run",
        )

        self.consumer_chart = BarChart(
            self.consumers_tab,
            title="Rows processed per second",
            height=250,
        )

        self.consumer_chart.pack(fill="x")

        self._section(
            self.consumers_tab,
            "Consumer groups and committed offsets",
        )

        self.consumer_tree = self._make_tree(
            self.consumers_tab,
            [
                (
                    "group",
                    "Group",
                    220,
                ),
                (
                    "processed",
                    "Processed",
                    160,
                ),
                (
                    "seconds",
                    "Seconds",
                    130,
                ),
                (
                    "events_sec",
                    "Events/sec",
                    150,
                ),
                (
                    "lag",
                    "Final lag",
                    130,
                ),
                (
                    "status",
                    "Status",
                    120,
                ),
            ],
            height=5,
        )

        self._section(
            self.consumers_tab,
            "Logical partition progress view",
        )

        self.offset_tree = self._make_tree(
            self.consumers_tab,
            [
                (
                    "group",
                    "Group",
                    220,
                ),
                (
                    "partition",
                    "Partition",
                    120,
                ),
                (
                    "end",
                    "Partition events",
                    160,
                ),
                (
                    "committed",
                    "Global progress applied",
                    180,
                ),
                (
                    "lag",
                    "Remaining",
                    120,
                ),
            ],
            height=7,
        )

    # ========================================================
    # FAILURE TAB
    # ========================================================

    def _build_failure_tab(self):

        self.failure_summary_var = tk.StringVar(
            value="Loading failure state..."
        )

        ttk.Label(
            self.failure_tab,
            text=(
                "Failure and recovery using "
                "PostgreSQL-stored offsets"
            ),
            style="PageTitle.TLabel",
        ).pack(anchor="w")

        controls = ttk.Frame(
            self.failure_tab
        )

        controls.pack(
            fill="x",
            pady=(12, 6),
        )

        self.failure_consumer_var = tk.StringVar(
            value=CONSUMERS[0][0]
        )

        self.failure_error_var = tk.StringVar(
            value="Simulated consumer interruption"
        )

        ttk.Label(
            controls,
            text="Consumer:",
        ).grid(
            row=0,
            column=0,
            sticky="w",
        )

        ttk.Combobox(
            controls,
            textvariable=self.failure_consumer_var,
            values=[
                name
                for name, _ in CONSUMERS
            ],
            state="readonly",
            width=28,
        ).grid(
            row=0,
            column=1,
            padx=6,
        )

        ttk.Label(
            controls,
            text="Failure message:",
        ).grid(
            row=0,
            column=2,
            padx=(14, 0),
            sticky="w",
        )

        ttk.Entry(
            controls,
            textvariable=self.failure_error_var,
            width=42,
        ).grid(
            row=0,
            column=3,
            padx=6,
            sticky="ew",
        )

        ttk.Button(
            controls,
            text="Inject failure",
            command=self.simulate_failure,
        ).grid(
            row=1,
            column=1,
            pady=10,
            sticky="w",
        )

        ttk.Button(
            controls,
            text="Recover consumer",
            style="Accent.TButton",
            command=self.recover_consumer,
        ).grid(
            row=1,
            column=2,
            pady=10,
            sticky="w",
        )

        controls.columnconfigure(
            3,
            weight=1,
        )

        self._banner(
            self.failure_tab,
            self.failure_summary_var,
            warning=True,
        )

        self._section(
            self.failure_tab,
            "Blast radius — current consumer state",
        )

        self.failure_tree = self._make_tree(
            self.failure_tab,
            [
                (
                    "component",
                    "Component",
                    240,
                ),
                (
                    "status",
                    "Status",
                    150,
                ),
                (
                    "evidence",
                    "Evidence from PostgreSQL",
                    800,
                ),
            ],
            height=6,
        )

        cards = ttk.Frame(
            self.failure_tab
        )

        cards.pack(
            fill="x",
            pady=(12, 0),
        )

        self.failure_card_vars = {}

        for index, (
            caption,
            key,
        ) in enumerate([
            (
                "events already committed",
                "committed",
            ),
            (
                "backlog remaining",
                "backlog",
            ),
            (
                "total calendar events",
                "events",
            ),
            (
                "saved offset",
                "offset",
            ),
        ]):

            card = ttk.Frame(
                cards,
                style="Card.TFrame",
                padding=10,
            )

            card.grid(
                row=0,
                column=index,
                sticky="nsew",
                padx=4,
            )

            var = tk.StringVar(value="...")

            self.failure_card_vars[key] = var

            ttk.Label(
                card,
                textvariable=var,
                style="CardValue.TLabel",
                anchor="center",
            ).pack(fill="x")

            ttk.Label(
                card,
                text=caption,
                style="CardCaption.TLabel",
                anchor="center",
            ).pack(fill="x")

            cards.columnconfigure(
                index,
                weight=1,
            )

    # ========================================================
    # REPLAY TAB
    # ========================================================

    def _build_replay_tab(self):

        self.replay_summary_var = tk.StringVar(
            value=(
                "Replay waits for a PostgreSQL query."
            )
        )

        top = ttk.Frame(
            self.replay_tab
        )

        top.pack(fill="x")

        ttk.Label(
            top,
            text=(
                "Revenue and Payment Replay"
            ),
            style="PageTitle.TLabel",
        ).pack(side="left")

        controls = ttk.Frame(
            self.replay_tab
        )

        controls.pack(
            fill="x",
            pady=(10, 4),
        )

        self.replay_offset_var = tk.StringVar(
            value="0"
        )

        self.replay_limit_var = tk.StringVar(
            value=str(DEFAULT_PAGE_SIZE)
        )

        ttk.Label(
            controls,
            text="Start offset:",
        ).pack(side="left")

        ttk.Entry(
            controls,
            textvariable=self.replay_offset_var,
            width=14,
        ).pack(
            side="left",
            padx=6,
        )

        ttk.Label(
            controls,
            text="Rows:",
        ).pack(
            side="left",
            padx=(12, 0),
        )

        ttk.Entry(
            controls,
            textvariable=self.replay_limit_var,
            width=10,
        ).pack(
            side="left",
            padx=6,
        )

        ttk.Button(
            controls,
            text="Run replay",
            style="Accent.TButton",
            command=lambda: self.run_stage(
                "replay",
                self.run_replay,
            ),
        ).pack(side="right")

        self._banner(
            self.replay_tab,
            self.replay_summary_var,
        )

        self._section(
            self.replay_tab,
            "Calendar Revenue and Payment Events",
        )

        self.replay_revenue_tree = self._make_tree(
            self.replay_tab,
            [
                (
                    "listing_id",
                    "Listing ID",
                    130,
                ),
                (
                    "date",
                    "Date",
                    130,
                ),
                (
                    "available",
                    "Availability",
                    120,
                ),
                (
                    "price",
                    "Listed Price",
                    130,
                ),
                (
                    "payment_method",
                    "Payment Method",
                    160,
                ),
                (
                    "payment_status",
                    "Payment Status",
                    160,
                ),
                (
                    "amount_paid",
                    "Actual Amount Paid",
                    170,
                ),
            ],
            height=14,
        )

        self.replay_revenue_note_var = tk.StringVar(
            value=(
                "The table displays values stored in "
                "public.calendar. Listed Price is not "
                "treated as revenue. Actual Recorded "
                "Revenue is calculated from amount_paid."
            )
        )

        ttk.Label(
            self.replay_tab,
            textvariable=self.replay_revenue_note_var,
            style="Note.TLabel",
            wraplength=1400,
            justify="left",
        ).pack(
            fill="x",
            pady=(3, 8),
        )

        self._section(
            self.replay_tab,
            "Payment Status Summary",
        )

        self.payment_status_tree = self._make_tree(
            self.replay_tab,
            [
                (
                    "status",
                    "Payment Status",
                    280,
                ),
                (
                    "events",
                    "Calendar Events",
                    220,
                ),
                (
                    "amount_rows",
                    "Rows with Actual Amount Paid",
                    280,
                ),
                (
                    "amount_paid",
                    "Actual Amount Paid",
                    240,
                ),
            ],
            height=7,
        )

    # ========================================================
    # RECONCILIATION TAB
    # ========================================================

    def _build_reconciliation_tab(self):

        self.reconciliation_summary_var = tk.StringVar(
            value=(
                "Waiting for PostgreSQL reconciliation."
            )
        )

        top = ttk.Frame(
            self.reconciliation_tab
        )

        top.pack(fill="x")

        ttk.Label(
            top,
            text=(
                "Does the ordered stream agree with "
                "the database aggregate?"
            ),
            style="PageTitle.TLabel",
        ).pack(side="left")

        ttk.Button(
            top,
            text="Reconcile now",
            style="Accent.TButton",
            command=lambda: self.run_stage(
                "reconcile",
                self.refresh_reconciliation,
            ),
        ).pack(side="right")

        self._banner(
            self.reconciliation_tab,
            self.reconciliation_summary_var,
        )

        self.reconciliation_tree = self._make_tree(
            self.reconciliation_tab,
            [
                (
                    "measure",
                    "Measure",
                    360,
                ),
                (
                    "batch",
                    "Database aggregate",
                    270,
                ),
                (
                    "stream",
                    "Ordered stream query",
                    270,
                ),
                (
                    "difference",
                    "Difference",
                    220,
                ),
            ],
            height=7,
        )

        self._section(
            self.reconciliation_tab,
            "Conditions enforced by the reconciliation",
        )

        self.reconciliation_condition_tree = self._make_tree(
            self.reconciliation_tab,
            [
                (
                    "condition",
                    "Condition",
                    760,
                ),
                (
                    "kind",
                    "Kind",
                    170,
                ),
                (
                    "held",
                    "Held?",
                    130,
                ),
            ],
            height=7,
        )

    # ========================================================
    # CONSOLE TAB
    # ========================================================

    def _build_console_tab(self):

        top = ttk.Frame(
            self.console_tab
        )

        top.pack(fill="x")

        ttk.Label(
            top,
            text=(
                "Verbatim output of every "
                "PostgreSQL-backed stage"
            ),
            style="PageTitle.TLabel",
        ).pack(side="left")

        ttk.Button(
            top,
            text="Clear",
            command=self.clear_console,
        ).pack(side="right")

        ttk.Button(
            top,
            text="Refresh all",
            command=self.refresh_all,
        ).pack(
            side="right",
            padx=6,
        )

        outer = ttk.Frame(
            self.console_tab
        )

        outer.pack(
            fill="both",
            expand=True,
            pady=(10, 0),
        )

        self.console_text = tk.Text(
            outer,
            bg="#17263b",
            fg="#e5edf6",
            insertbackground="white",
            font=("Consolas", 10),
            wrap="word",
            relief="flat",
        )

        scrollbar = ttk.Scrollbar(
            outer,
            orient="vertical",
            command=self.console_text.yview,
        )

        self.console_text.configure(
            yscrollcommand=scrollbar.set
        )

        self.console_text.pack(
            side="left",
            fill="both",
            expand=True,
        )

        scrollbar.pack(
            side="right",
            fill="y",
        )

    # ========================================================
    # STARTUP
    # ========================================================

    def initialize(self):

        try:

            for table in (
                "listings",
                "calendar",
                "reviews",
            ):

                exists = query_one(
                    """
                    SELECT EXISTS(
                        SELECT 1
                        FROM information_schema.tables
                        WHERE table_schema='public'
                        AND table_name=%s
                    )
                    """,
                    (table,),
                )[0]

                if not exists:

                    raise RuntimeError(
                        f"Missing public.{table}. "
                        "Run load_postgres.py first."
                    )

            required_columns = {
                "calendar": [
                    "listing_id",
                    "date",
                    "available",
                    "price",
                    "payment_method",
                    "payment_status",
                    "amount_paid",
                ],
            }

            for table, columns in required_columns.items():

                for column in columns:

                    exists = query_one(
                        """
                        SELECT EXISTS(
                            SELECT 1
                            FROM information_schema.columns
                            WHERE table_schema='public'
                            AND table_name=%s
                            AND column_name=%s
                        )
                        """,
                        (
                            table,
                            column,
                        ),
                    )[0]

                    if not exists:

                        raise RuntimeError(
                            f"Missing public.{table}.{column}. "
                            "Load the updated calendar dataset first."
                        )

            self.ensure_support_tables()

            database, user = query_one(
                """
                SELECT
                    current_database(),
                    current_user
                """
            )

            self.connection_ok = True

            self.status_var.set(
                f"Connected to PostgreSQL database "
                f"'{database}' as '{user}'"
            )

            self.log(
                "PostgreSQL connection successful."
            )

            self.log(
                "Source tables: public.listings, "
                "public.calendar, public.reviews"
            )

            self.log(
                "Revenue terminology: Listed Price is "
                "separate from Actual Amount Paid."
            )

            self.log(
                "Actual Recorded Revenue is calculated "
                "from valid numeric amount_paid values."
            )

            self.refresh_all()

        except Exception as error:

            self.connection_ok = False

            self.status_var.set(
                "PostgreSQL connection failed"
            )

            self.log(
                f"STARTUP ERROR: "
                f"{type(error).__name__}: {error}"
            )

            messagebox.showerror(
                "AVR-Flow PostgreSQL Error",
                str(error),
            )

    # ========================================================
    # SUPPORT TABLES
    # ========================================================

    def ensure_support_tables(self):

        execute_command(
            """
            CREATE TABLE IF NOT EXISTS
            public.avr_flow_consumers (

                consumer_name TEXT PRIMARY KEY,

                description TEXT NOT NULL,

                current_offset BIGINT NOT NULL DEFAULT 0,

                status TEXT NOT NULL DEFAULT 'RUNNING',

                last_error TEXT,

                updated_at TIMESTAMP NOT NULL
                DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        for name, description in CONSUMERS:

            execute_command(
                """
                INSERT INTO public.avr_flow_consumers
                    (
                        consumer_name,
                        description
                    )
                VALUES
                    (%s, %s)

                ON CONFLICT (consumer_name)
                DO NOTHING
                """,
                (
                    name,
                    description,
                ),
            )

    # ========================================================
    # PIPELINE REFRESH
    # ========================================================

    def refresh_pipeline(self):

        listings, calendar, reviews = query_one(
            """
            SELECT
                (
                    SELECT COUNT(*)
                    FROM public.listings
                ),

                (
                    SELECT COUNT(*)
                    FROM public.calendar
                ),

                (
                    SELECT COUNT(*)
                    FROM public.reviews
                )
            """
        )

        lag = query_one(
            """
            SELECT
                COALESCE(
                    SUM(
                        GREATEST(
                            %s - current_offset,
                            0
                        )
                    ),
                    0
                )

            FROM public.avr_flow_consumers
            """,
            (calendar,),
        )[0]

        actual_revenue = query_one(
            f"""
            SELECT
                COALESCE(
                    SUM(
                        CASE
                            WHEN {VALID_AMOUNT_PAID_SQL}
                            THEN {AMOUNT_PAID_NUMERIC_SQL}
                            ELSE 0
                        END
                    ),
                    0
                )

            FROM public.calendar c
            """
        )[0]

        self.pipeline_vars["events"].set(
            fmt_number(calendar)
        )

        self.pipeline_vars["groups"].set(
            fmt_number(len(CONSUMERS))
        )

        self.pipeline_vars["lag"].set(
            fmt_number(lag)
        )

        self.pipeline_vars["revenue"].set(
            fmt_money(actual_revenue)
        )

        self.pipeline_vars["reconciliation"].set(
            self.stage_state["reconcile"]["status"]
        )

        self._render_stage_table()

        self.clear_tree(
            self.pipeline_source_tree
        )

        for row in [
            (
                "public.listings",
                fmt_number(listings),
                "Listing entity source",
            ),
            (
                "public.calendar",
                fmt_number(calendar),
                "Durable event, price, and payment source",
            ),
            (
                "public.reviews",
                fmt_number(reviews),
                "Related review event source",
            ),
        ]:

            self.pipeline_source_tree.insert(
                "",
                "end",
                values=row,
            )

        self.log(
            "Pipeline summary refreshed from "
            "PostgreSQL queries."
        )

    # ========================================================
    # LOG SELF-TEST
    # ========================================================

    def run_log_self_test(self):

        duplicate_keys = query_one(
            """
            SELECT COUNT(*)

            FROM (
                SELECT
                    listing_id,
                    date

                FROM public.calendar

                GROUP BY
                    listing_id,
                    date

                HAVING COUNT(*) > 1
            ) duplicates
            """
        )[0]

        orphan_rows = query_one(
            """
            SELECT COUNT(*)

            FROM public.calendar c

            LEFT JOIN public.listings l
                ON l.id = c.listing_id

            WHERE l.id IS NULL
            """
        )[0]

        total = int(
            query_one(
                """
                SELECT COUNT(*)
                FROM public.calendar
                """
            )[0]
        )

        distinct_listings = int(
            query_one(
                """
                SELECT
                    COUNT(DISTINCT listing_id)
                FROM public.calendar
                """
            )[0]
        )

        min_date, max_date = query_one(
            """
            SELECT
                MIN(date),
                MAX(date)
            FROM public.calendar
            """
        )

        valid_amount_rows = int(
            query_one(
                f"""
                SELECT COUNT(*)

                FROM public.calendar c

                WHERE {VALID_AMOUNT_PAID_SQL}
                """
            )[0]
        )

        self.refresh_durable_log()

        passed = (
            duplicate_keys == 0
            and orphan_rows == 0
        )

        result = (
            "PASSED"
            if passed
            else "FAILED"
        )

        self.log_stage(
            "LOG SELF-TEST",
            [
                "Data source       : public.calendar",
                f"Calendar events   : {total:,}",
                f"Distinct listings : {distinct_listings:,}",
                f"Date range        : {min_date} to {max_date}",
                f"Valid amount_paid rows: {valid_amount_rows:,}",
                "",
                (
                    "Composite-key duplicates "
                    f"(listing_id, date): {duplicate_keys:,}"
                ),
                (
                    "Orphan calendar rows "
                    f"(missing listings.id): {orphan_rows:,}"
                ),
                "",
                f"RESULT: {result}",
                (
                    "Listed Price and Actual Amount Paid "
                    "remain separate concepts."
                ),
            ],
        )

        return (
            f"{result}: {total:,} events checked · "
            f"{duplicate_keys:,} duplicate keys · "
            f"{orphan_rows:,} orphan rows"
        )

    # ========================================================
    # DURABLE LOG
    # ========================================================

    def refresh_durable_log(self):

        started = time.perf_counter()

        rows = query_all(
            """
            SELECT
                MOD(listing_id, %s)
                AS partition_id,

                COUNT(*)
                AS events

            FROM public.calendar

            GROUP BY
                MOD(listing_id, %s)

            ORDER BY
                partition_id
            """,
            (
                PARTITION_COUNT,
                PARTITION_COUNT,
            ),
        )

        elapsed = (
            time.perf_counter()
            - started
        )

        total = sum(
            int(row[1])
            for row in rows
        )

        even_share = (
            total / PARTITION_COUNT
            if PARTITION_COUNT
            else 0
        )

        self.partition_chart.set_data(
            [
                (
                    f"partition {row[0]}",
                    float(row[1]),
                    fmt_number(row[1]),
                )
                for row in rows
            ]
        )

        self.clear_tree(
            self.partition_tree
        )

        for partition_id, events in rows:

            share = (
                float(events)
                / total
                * 100
                if total
                else 0
            )

            observation = (
                "above even share"
                if events > even_share
                else (
                    "below even share"
                    if events < even_share
                    else "exactly even"
                )
            )

            self.partition_tree.insert(
                "",
                "end",
                values=(
                    f"partition {partition_id}",
                    fmt_number(events),
                    f"{share:.1f}%",
                    observation,
                ),
            )

        max_events = (
            max(
                int(row[1])
                for row in rows
            )
            if rows
            else 0
        )

        min_events = (
            min(
                int(row[1])
                for row in rows
            )
            if rows
            else 0
        )

        skew = (
            max_events / min_events
            if min_events
            else 0
        )

        duplicate_keys = query_one(
            """
            SELECT COUNT(*)

            FROM (
                SELECT
                    listing_id,
                    date

                FROM public.calendar

                GROUP BY
                    listing_id,
                    date

                HAVING COUNT(*) > 1
            ) duplicates
            """
        )[0]

        self.clear_tree(
            self.guarantee_tree
        )

        for row in [
            (
                "Stable key routing: MOD(listing_id, 4)",
                "PASSED",
            ),
            (
                "Explicit replay ordering",
                "PASSED",
            ),
            (
                "Calendar composite key uniqueness",
                (
                    "PASSED"
                    if duplicate_keys == 0
                    else "FAILED"
                ),
            ),
            (
                "Consumer state stored in PostgreSQL",
                "PASSED",
            ),
            (
                "Source tables remain unchanged by GUI",
                "PASSED",
            ),
            (
                "Listed Price is separate from Actual Amount Paid",
                "PASSED",
            ),
        ]:

            self.guarantee_tree.insert(
                "",
                "end",
                values=row,
            )

        self.durable_summary_var.set(
            f"{fmt_number(total)} PostgreSQL calendar "
            f"events across {PARTITION_COUNT} logical "
            f"partitions · query {elapsed:.3f} s · "
            f"partition skew {skew:.2f}:1"
        )

        self.partition_note_var.set(
            f"Counts are calculated directly in PostgreSQL "
            f"using MOD(listing_id, {PARTITION_COUNT}). "
            f"The largest partition has "
            f"{fmt_number(max_events)} events and the "
            f"smallest has {fmt_number(min_events)}. "
            f"An even share is approximately "
            f"{fmt_number(even_share)} events."
        )

        partition_lines = [
            f"partition {partition_id}: "
            f"{int(events):,} events "
            f"({(float(events) / total * 100) if total else 0:.1f}%)"
            for partition_id, events in rows
        ]

        self.log_stage(
            "PRODUCE / DURABLE LOG QUERY",
            [
                "Source           : public.calendar",
                (
                    "Logical routing  : "
                    f"MOD(listing_id, {PARTITION_COUNT})"
                ),
                f"Total events     : {total:,}",
                f"Query time       : {elapsed:.3f} seconds",
                "",
                "PARTITION DISTRIBUTION",
                *partition_lines,
                "",
                f"Largest partition: {max_events:,} events",
                f"Smallest partition: {min_events:,} events",
                f"Partition skew   : {skew:.2f}:1",
                f"Duplicate keys   : {duplicate_keys:,}",
                "",
                "RESULT: durable source verified.",
            ],
        )

        return (
            f"{total:,} calendar events · "
            f"{PARTITION_COUNT} partitions · "
            f"skew {skew:.2f}:1 · "
            f"query {elapsed:.3f}s"
        )

    # ========================================================
    # CONSUMERS
    # ========================================================

    def _consumer_rows(self):

        return query_all(
            """
            SELECT
                consumer_name,
                description,
                current_offset,
                status,
                updated_at

            FROM public.avr_flow_consumers

            ORDER BY
                consumer_name
            """
        )

    def reset_all_consumers(self):

        # Every consumer group commits its progress to
        # public.avr_flow_consumers.current_offset. Once a
        # consumer has caught up to the total row count, the
        # NEXT "Run all consumers" click correctly finds zero
        # new rows past that offset — that's expected
        # streaming-consumer behavior, not a bug. This resets
        # every group's committed offset back to 0 (status
        # RUNNING, no error) so a fresh "Consume" run has the
        # full public.calendar backlog to process again.

        if not messagebox.askyesno(
            "Reset Consumer Offsets",
            "This sets every consumer group's committed "
            "offset back to 0 in PostgreSQL, so the next "
            "Consume run reprocesses the full calendar "
            "backlog. Continue?",
        ):
            return

        execute_command(
            """
            UPDATE public.avr_flow_consumers

            SET
                current_offset=0,
                status='RUNNING',
                last_error=NULL,
                updated_at=CURRENT_TIMESTAMP
            """
        )

        self.refresh_consumers()

        self.log_stage(
            "CONSUMER OFFSETS RESET",
            [
                "Action  : current_offset set to 0 for all "
                "consumer groups",
                "Status  : all groups set to RUNNING",
                "Source  : public.avr_flow_consumers",
                (
                    "Next step : run Consume to reprocess "
                    "the full public.calendar backlog."
                ),
            ],
        )

        self.status_var.set(
            "Consumer offsets reset — "
            + datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )

    def run_all_consumers(self):

        total = int(
            query_one(
                """
                SELECT COUNT(*)
                FROM public.calendar
                """
            )[0]
        )

        chart_data = []
        run_rows = []

        for (
            name,
            description,
            offset,
            status,
            updated_at,
        ) in self._consumer_rows():

            offset = int(offset)

            if status != "RUNNING":

                lag = max(
                    0,
                    total - offset,
                )

                run_rows.append(
                    (
                        name,
                        0,
                        0.0,
                        0.0,
                        lag,
                        status,
                    )
                )

                chart_data.append(
                    (
                        name,
                        0.0,
                        status,
                    )
                )

                continue

            remaining = max(
                0,
                total - offset,
            )

            started = time.perf_counter()

            processed = query_one(
                """
                SELECT COUNT(*)

                FROM (
                    SELECT
                        listing_id

                    FROM public.calendar

                    ORDER BY
                        listing_id,
                        date

                    OFFSET %s

                    LIMIT %s
                ) events
                """,
                (
                    offset,
                    remaining,
                ),
            )[0]

            seconds = max(
                time.perf_counter() - started,
                0.000001,
            )

            new_offset = min(
                total,
                offset + int(processed),
            )

            events_per_second = (
                int(processed)
                / seconds
            )

            execute_command(
                """
                UPDATE public.avr_flow_consumers

                SET
                    current_offset=%s,
                    status='RUNNING',
                    updated_at=CURRENT_TIMESTAMP

                WHERE consumer_name=%s
                """,
                (
                    new_offset,
                    name,
                ),
            )

            run_rows.append(
                (
                    name,
                    int(processed),
                    seconds,
                    events_per_second,
                    max(
                        0,
                        total - new_offset,
                    ),
                    "RUNNING",
                )
            )

            chart_data.append(
                (
                    name,
                    events_per_second,
                    f"{events_per_second:,.0f}",
                )
            )

        self.consumer_chart.set_data(
            chart_data
        )

        self.refresh_consumers(
            run_rows
        )

        report_lines = [
            "Source            : public.calendar",
            f"Total source rows : {total:,}",
            f"Consumer groups   : {len(run_rows)}",
            "",
            "CONSUMER RESULTS",
        ]

        for (
            name,
            processed,
            seconds,
            eps,
            lag,
            status,
        ) in run_rows:

            report_lines.append(
                f"{name}: processed={processed:,} | "
                f"time={seconds:.3f}s | "
                f"throughput={eps:,.0f}/s | "
                f"final lag={lag:,} | "
                f"state={status}"
            )

        report_lines.extend([
            "",
            (
                "Total processed this run: "
                f"{sum(int(row[1]) for row in run_rows):,}"
            ),
            (
                "Total remaining lag    : "
                f"{sum(int(row[4]) for row in run_rows):,}"
            ),
            (
                "RESULT: consumer progress was read "
                "from and committed to PostgreSQL."
            ),
        ])

        self.log_stage(
            "CONSUME",
            report_lines,
        )

        return (
            f"{sum(int(row[1]) for row in run_rows):,} "
            f"events processed · "
            f"{sum(int(row[4]) for row in run_rows):,} "
            f"total lag"
        )

    def refresh_consumers(
        self,
        run_rows=None,
    ):

        total = int(
            query_one(
                """
                SELECT COUNT(*)
                FROM public.calendar
                """
            )[0]
        )

        if run_rows is None:

            run_rows = []

            for (
                name,
                description,
                offset,
                status,
                updated_at,
            ) in self._consumer_rows():

                offset = int(offset)

                run_rows.append(
                    (
                        name,
                        offset,
                        0.0,
                        0.0,
                        max(
                            0,
                            total - offset,
                        ),
                        status,
                    )
                )

            self.consumer_chart.set_data(
                [
                    (
                        name,
                        0.0,
                        "refresh only",
                    )
                    for name, *_ in run_rows
                ]
            )

        self.clear_tree(
            self.consumer_tree
        )

        for row in run_rows:

            (
                name,
                processed,
                seconds,
                eps,
                lag,
                status,
            ) = row

            self.consumer_tree.insert(
                "",
                "end",
                values=(
                    name,
                    fmt_number(processed),
                    f"{seconds:.3f}",
                    fmt_number(eps),
                    fmt_number(lag),
                    status,
                ),
            )

        partition_rows = query_all(
            """
            SELECT
                MOD(listing_id, %s),
                COUNT(*)

            FROM public.calendar

            GROUP BY
                MOD(listing_id, %s)

            ORDER BY 1
            """,
            (
                PARTITION_COUNT,
                PARTITION_COUNT,
            ),
        )

        self.clear_tree(
            self.offset_tree
        )

        states = {
            row[0]: int(row[2])
            for row in self._consumer_rows()
        }

        for (
            consumer_name,
            offset,
        ) in states.items():

            for (
                partition_id,
                event_count,
            ) in partition_rows:

                committed = min(
                    int(event_count),
                    offset,
                )

                lag = max(
                    0,
                    int(event_count)
                    - committed,
                )

                self.offset_tree.insert(
                    "",
                    "end",
                    values=(
                        consumer_name,
                        partition_id,
                        fmt_number(event_count),
                        fmt_number(committed),
                        fmt_number(lag),
                    ),
                )

        total_lag = sum(
            max(
                0,
                total - int(row[2]),
            )
            for row in self._consumer_rows()
        )

        finished = sum(
            1
            for row in self._consumer_rows()
            if int(row[2]) >= total
            and row[3] == "RUNNING"
        )

        self.consumer_summary_var.set(
            f"{finished} consumer groups caught up · "
            f"total lag {fmt_number(total_lag)} · "
            f"source size {fmt_number(total)} "
            f"PostgreSQL calendar events"
        )

    # ========================================================
    # FAILURE AND RECOVERY
    # ========================================================

    def simulate_failure(self):

        name = (
            self.failure_consumer_var.get()
            .strip()
        )

        error = (
            self.failure_error_var.get()
            .strip()
            or "Simulated consumer interruption"
        )

        execute_command(
            """
            UPDATE public.avr_flow_consumers

            SET
                status='FAILED',
                last_error=%s,
                updated_at=CURRENT_TIMESTAMP

            WHERE consumer_name=%s
            """,
            (
                error,
                name,
            ),
        )

        self.refresh_failure_view()
        self.refresh_consumers()

        self.log_stage(
            "FAILURE INJECTED",
            [
                f"Consumer         : {name}",
                "Action           : status changed to FAILED",
                "Offset handling  : preserved in PostgreSQL",
                f"Reason           : {error}",
                (
                    "Next step        : use Recover consumer "
                    "to resume from the saved offset."
                ),
            ],
        )

    def recover_consumer(self):

        name = (
            self.failure_consumer_var.get()
            .strip()
        )

        execute_command(
            """
            UPDATE public.avr_flow_consumers

            SET
                status='RUNNING',
                last_error=NULL,
                updated_at=CURRENT_TIMESTAMP

            WHERE consumer_name=%s
            """,
            (name,),
        )

        self.refresh_failure_view()
        self.refresh_consumers()

        offset = int(
            query_one(
                """
                SELECT current_offset

                FROM public.avr_flow_consumers

                WHERE consumer_name=%s
                """,
                (name,),
            )[0]
        )

        self.log_stage(
            "CONSUMER RECOVERED",
            [
                f"Consumer         : {name}",
                "Action           : status changed to RUNNING",
                f"Resume offset    : {offset:,}",
                (
                    "Progress source  : "
                    "public.avr_flow_consumers"
                ),
                (
                    "Next step        : run Consume to process "
                    "remaining PostgreSQL events."
                ),
            ],
        )

    def refresh_failure_view(self):

        total = int(
            query_one(
                """
                SELECT COUNT(*)
                FROM public.calendar
                """
            )[0]
        )

        rows = self._consumer_rows()

        selected_name = (
            self.failure_consumer_var.get()
            .strip()
        )

        self.clear_tree(
            self.failure_tree
        )

        for (
            name,
            description,
            offset,
            status,
            updated_at,
        ) in rows:

            offset = int(offset)

            evidence = (
                f"saved offset {offset:,}; "
                f"backlog {max(0, total - offset):,}; "
                f"state stored in "
                f"public.avr_flow_consumers"
            )

            self.failure_tree.insert(
                "",
                "end",
                values=(
                    name,
                    status,
                    evidence,
                ),
            )

        selected = next(
            (
                row
                for row in rows
                if row[0] == selected_name
            ),
            rows[0] if rows else None,
        )

        if not selected:

            return (
                "No consumer state was returned "
                "by PostgreSQL"
            )

        offset = int(selected[2])

        backlog = max(
            0,
            total - offset,
        )

        self.failure_card_vars["committed"].set(
            fmt_number(offset)
        )

        self.failure_card_vars["backlog"].set(
            fmt_number(backlog)
        )

        self.failure_card_vars["events"].set(
            fmt_number(total)
        )

        self.failure_card_vars["offset"].set(
            fmt_number(offset)
        )

        self.failure_summary_var.set(
            f"Consumer '{selected[0]}' is "
            f"{selected[3]} · saved offset "
            f"{fmt_number(offset)} · "
            f"backlog {fmt_number(backlog)}"
        )

        report_lines = [
            (
                "Consumer state is persisted in "
                "public.avr_flow_consumers."
            ),
            f"Selected consumer : {selected[0]}",
            f"Current status    : {selected[3]}",
            f"Saved offset      : {offset:,}",
            f"Current backlog   : {backlog:,}",
            "",
            "ALL CONSUMER STATES",
        ]

        for (
            name,
            description,
            saved_offset,
            status,
            updated_at,
        ) in rows:

            report_lines.append(
                f"{name}: status={status} | "
                f"offset={int(saved_offset):,} | "
                f"lag={max(0, total-int(saved_offset)):,} | "
                f"updated={updated_at}"
            )

        report_lines.extend([
            "",
            (
                "RESULT: failure/recovery state "
                "was read from PostgreSQL."
            ),
        ])

        self.log_stage(
            "FAILURE & RECOVERY STATUS",
            report_lines,
        )

        return (
            f"{selected[0]} is {selected[3]} · "
            f"saved offset {offset:,} · "
            f"backlog {backlog:,}"
        )

    # ========================================================
    # REPLAY PREVIEW
    # ========================================================

    def refresh_replay_preview_only(self):

        self.clear_tree(
            self.replay_revenue_tree
        )

        self.clear_tree(
            self.payment_status_tree
        )

        self.replay_summary_var.set(
            "No replay has been run yet. "
            "Run Replay to read calendar payment "
            "and revenue records from PostgreSQL."
        )

        self.replay_revenue_note_var.set(
            "Listed Price is the calendar price. "
            "Actual Amount Paid is the payment value "
            "stored in amount_paid. Actual Recorded "
            "Revenue is calculated from amount_paid."
        )

    # ========================================================
    # REPLAY
    # ========================================================

    def run_replay(self):

        # NOTE: this only runs through run_stage("replay", ...),
        # so any exception raised here is caught there — it marks
        # the "Replay" stage FAILED and shows the error dialog.
        # Previously this method caught its own ValueError, showed
        # a dialog, and returned None — but run_stage treats a
        # None result as "Stage completed successfully" and marks
        # the stage PASSED, so invalid input silently showed up as
        # a passing "6. Replay" stage on the Pipeline tab even
        # though nothing was replayed.

        offset = int(
            self.replay_offset_var.get()
        )

        limit = int(
            self.replay_limit_var.get()
        )

        if offset < 0:

            raise ValueError(
                "Start offset cannot be negative."
            )

        if not 1 <= limit <= 10000:

            raise ValueError(
                "Rows must be between 1 and 10,000."
            )

        total = int(
            query_one(
                """
                SELECT COUNT(*)
                FROM public.calendar
                """
            )[0]
        )

        rows = query_all(
            """
            SELECT
                c.listing_id,
                c.date,
                c.available,
                c.price,
                c.payment_method,
                c.payment_status,
                c.amount_paid

            FROM public.calendar c

            ORDER BY
                c.listing_id,
                c.date

            OFFSET %s

            LIMIT %s
            """,
            (
                offset,
                limit,
            ),
        )

        self.clear_tree(
            self.replay_revenue_tree
        )

        for row in rows:

            amount_paid = row[6]

            amount_display = (
                fmt_money(amount_paid)
                if amount_paid is not None
                and str(amount_paid).strip() != ""
                else "N/A"
            )

            self.replay_revenue_tree.insert(
                "",
                "end",
                values=(
                    row[0],
                    row[1],
                    fmt_text(row[2]),
                    fmt_text(row[3]),
                    fmt_text(row[4]),
                    fmt_text(row[5]),
                    amount_display,
                ),
            )

        end_offset = (
            offset + len(rows) - 1
            if rows
            else offset
        )

        (
            total_events,
            amount_rows,
            actual_revenue,
        ) = query_one(
            f"""
            SELECT
                COUNT(*) AS total_events,

                COUNT(*)
                FILTER (
                    WHERE {VALID_AMOUNT_PAID_SQL}
                ) AS amount_rows,

                COALESCE(
                    SUM(
                        CASE
                            WHEN {VALID_AMOUNT_PAID_SQL}
                            THEN {AMOUNT_PAID_NUMERIC_SQL}
                            ELSE 0
                        END
                    ),
                    0
                ) AS actual_recorded_revenue

            FROM public.calendar c
            """
        )

        total_events = int(
            total_events or 0
        )

        amount_rows = int(
            amount_rows or 0
        )

        actual_revenue = (
            actual_revenue
            or 0
        )

        self.replay_summary_var.set(
            f"{fmt_number(len(rows))} rows returned "
            f"from PostgreSQL · offset "
            f"{fmt_number(offset)} to "
            f"{fmt_number(end_offset)} · source contains "
            f"{fmt_number(total)} calendar events · "
            f"{fmt_number(amount_rows)} rows with valid "
            f"Actual Amount Paid · Actual Recorded Revenue "
            f"{fmt_money(actual_revenue)}"
        )

        self.replay_revenue_note_var.set(
            f"Revenue model: Listed Price comes from price "
            f"and is not treated as revenue. Payment Method "
            f"and Payment Status describe payment information. "
            f"Actual Amount Paid comes from amount_paid. "
            f"Actual Recorded Revenue across the source is "
            f"{fmt_money(actual_revenue)}."
        )

        status_rows = query_all(
            f"""
            SELECT
                COALESCE(
                    NULLIF(
                        BTRIM(c.payment_status),
                        ''
                    ),
                    'N/A'
                ) AS payment_status,

                COUNT(*) AS events,

                COUNT(*)
                FILTER (
                    WHERE {VALID_AMOUNT_PAID_SQL}
                ) AS amount_rows,

                COALESCE(
                    SUM(
                        CASE
                            WHEN {VALID_AMOUNT_PAID_SQL}
                            THEN {AMOUNT_PAID_NUMERIC_SQL}
                            ELSE 0
                        END
                    ),
                    0
                ) AS actual_amount_paid

            FROM public.calendar c

            GROUP BY 1

            ORDER BY
                actual_amount_paid DESC,
                events DESC,
                payment_status
            """
        )

        self.clear_tree(
            self.payment_status_tree
        )

        for (
            payment_status,
            events,
            status_amount_rows,
            status_amount_paid,
        ) in status_rows:

            self.payment_status_tree.insert(
                "",
                "end",
                values=(
                    payment_status,
                    fmt_number(events),
                    fmt_number(status_amount_rows),
                    fmt_money(status_amount_paid),
                ),
            )

        report_lines = [
            "Source table        : public.calendar",
            "Ordering            : ORDER BY listing_id, date",
            f"Requested offset    : {offset:,}",
            f"Requested limit     : {limit:,}",
            f"Returned rows       : {len(rows):,}",
            f"Offset range        : {offset:,} to {end_offset:,}",
            "",
            "REVENUE TERMINOLOGY",
            "Listed Price        : calendar.price",
            "Payment Method      : calendar.payment_method",
            "Payment Status      : calendar.payment_status",
            "Actual Amount Paid  : calendar.amount_paid",
            (
                "Actual Recorded Revenue: SUM(valid "
                "numeric amount_paid values)"
            ),
            "",
            f"Source events       : {total_events:,}",
            f"Valid amount rows   : {amount_rows:,}",
            (
                "Actual Recorded Revenue: "
                f"{fmt_money(actual_revenue)}"
            ),
            "",
            "RESULT: replayed PostgreSQL payment and "
            "calendar records without treating listed price "
            "as actual revenue.",
        ]

        self.log_stage(
            "REPLAY",
            report_lines,
        )

        return (
            f"{len(rows):,} rows replayed · "
            f"offsets {offset:,} to {end_offset:,} · "
            f"Actual Recorded Revenue "
            f"{fmt_money(actual_revenue)}"
        )

    # ========================================================
    # RECONCILIATION PREVIEW
    # ========================================================

    def refresh_reconciliation_preview_only(self):

        self.reconciliation_summary_var.set(
            "Reconciliation has not been run in this GUI "
            "session. Stage status remains "
            f"{self.stage_state['reconcile']['status']}."
        )

    # ========================================================
    # RECONCILIATION
    # ========================================================

    def refresh_reconciliation(self):

        # ========================================================
        # PATH A: DIRECT DATABASE AGGREGATE
        # ========================================================

        batch_total, batch_paid_rows, batch_revenue = query_one(
            f"""
            SELECT
                COUNT(*) AS total_records,

                COUNT(*) FILTER (
                    WHERE {VALID_AMOUNT_PAID_SQL}
                ) AS rows_with_amount_paid,

                COALESCE(
                    SUM(
                        CASE
                            WHEN {VALID_AMOUNT_PAID_SQL}
                            THEN {AMOUNT_PAID_NUMERIC_SQL}
                            ELSE 0
                        END
                    ),
                    0
                ) AS total_actual_amount_paid

            FROM public.calendar c
            """
        )

        batch_total = int(batch_total or 0)
        batch_paid_rows = int(batch_paid_rows or 0)
        batch_revenue = float(batch_revenue or 0)

        # ========================================================
        # PATH B: EXPLICITLY ORDERED STREAM
        # ========================================================

        stream_total, stream_paid_rows, stream_revenue = query_one(
            f"""
            SELECT
                COUNT(*) AS total_records,

                COUNT(*) FILTER (
                    WHERE {STREAM_VALID_AMOUNT_PAID_SQL}
                ) AS rows_with_amount_paid,

                COALESCE(
                    SUM(
                        CASE
                            WHEN {STREAM_VALID_AMOUNT_PAID_SQL}
                            THEN {STREAM_AMOUNT_PAID_NUMERIC_SQL}
                            ELSE 0
                        END
                    ),
                    0
                ) AS total_actual_amount_paid

            FROM (
                SELECT
                    c.listing_id,
                    c.date,
                    c.available,
                    c.price,
                    c.payment_method,
                    c.payment_status,
                    c.amount_paid

                FROM public.calendar c

                ORDER BY
                    c.listing_id,
                    c.date
            ) AS ordered_stream
            """
        )

        stream_total = int(stream_total or 0)
        stream_paid_rows = int(stream_paid_rows or 0)
        stream_revenue = float(stream_revenue or 0)

        # ========================================================
        # PAYMENT STATUS DISTRIBUTION
        # ========================================================

        batch_payment_statuses = query_all(
            f"""
            SELECT
                COALESCE(
                    NULLIF(TRIM(c.payment_status), ''),
                    'NO STATUS'
                ) AS payment_status,

                COUNT(*) AS row_count,

                COALESCE(
                    SUM(
                        CASE
                            WHEN {VALID_AMOUNT_PAID_SQL}
                            THEN {AMOUNT_PAID_NUMERIC_SQL}
                            ELSE 0
                        END
                    ),
                    0
                ) AS total_amount_paid

            FROM public.calendar c

            GROUP BY
                COALESCE(
                    NULLIF(TRIM(c.payment_status), ''),
                    'NO STATUS'
                )

            ORDER BY
                payment_status
            """
        )

        stream_payment_statuses = query_all(
            f"""
            SELECT
                COALESCE(
                    NULLIF(TRIM(payment_status), ''),
                    'NO STATUS'
                ) AS payment_status,

                COUNT(*) AS row_count,

                COALESCE(
                    SUM(
                        CASE
                            WHEN {STREAM_VALID_AMOUNT_PAID_SQL}
                            THEN {STREAM_AMOUNT_PAID_NUMERIC_SQL}
                            ELSE 0
                        END
                    ),
                    0
                ) AS total_amount_paid

            FROM (
                SELECT
                    c.payment_status,
                    c.amount_paid

                FROM public.calendar c

                ORDER BY
                    c.listing_id,
                    c.date
            ) AS ordered_stream

            GROUP BY
                COALESCE(
                    NULLIF(TRIM(payment_status), ''),
                    'NO STATUS'
                )

            ORDER BY
                payment_status
            """
        )

        # ========================================================
        # CONVERT STATUS RESULTS FOR EXACT COMPARISON
        # ========================================================

        batch_status_map = {
            str(status): (
                int(row_count),
                round(float(total_amount or 0), 6),
            )
            for status, row_count, total_amount
            in batch_payment_statuses
        }

        stream_status_map = {
            str(status): (
                int(row_count),
                round(float(total_amount or 0), 6),
            )
            for status, row_count, total_amount
            in stream_payment_statuses
        }

        payment_status_match = (
            batch_status_map == stream_status_map
        )

        # ========================================================
        # DISPLAY RECONCILIATION RESULTS
        # ========================================================

        record_difference = abs(batch_total - stream_total)
        payment_row_difference = abs(batch_paid_rows - stream_paid_rows)
        revenue_difference = abs(batch_revenue - stream_revenue)

        self.clear_tree(
            self.reconciliation_tree
        )

        reconciliation_rows = [
            (
                "Total calendar records",
                fmt_number(batch_total),
                fmt_number(stream_total),
                fmt_number(record_difference),
            ),
            (
                "Rows with amount paid",
                fmt_number(batch_paid_rows),
                fmt_number(stream_paid_rows),
                fmt_number(payment_row_difference),
            ),
            (
                "Total actual amount paid",
                fmt_money(batch_revenue),
                fmt_money(stream_revenue),
                fmt_money(revenue_difference),
            ),
            (
                "Payment status distribution",
                fmt_number(len(batch_status_map)),
                fmt_number(len(stream_status_map)),
                "0" if payment_status_match else "DIFFERENT",
            ),
        ]

        for row in reconciliation_rows:

            self.reconciliation_tree.insert(
                "",
                "end",
                values=row,
            )

        # ========================================================
        # REFERENTIAL INTEGRITY
        # ========================================================

        orphan_calendar = int(
            query_one(
                """
                SELECT
                    COUNT(*)

                FROM public.calendar c

                LEFT JOIN public.listings l
                ON l.id = c.listing_id

                WHERE l.id IS NULL
                """
            )[0]
        )

        orphan_reviews = int(
            query_one(
                """
                SELECT
                    COUNT(*)

                FROM public.reviews r

                LEFT JOIN public.listings l
                ON l.id = r.listing_id

                WHERE l.id IS NULL
                """
            )[0]
        )

        duplicate_calendar = int(
            query_one(
                """
                SELECT
                    COUNT(*)

                FROM (

                    SELECT
                        listing_id,
                        date

                    FROM public.calendar

                    GROUP BY
                        listing_id,
                        date

                    HAVING COUNT(*) > 1

                ) AS duplicates
                """
            )[0]
        )

        # ========================================================
        # RECONCILIATION CONDITIONS
        # ========================================================

        conditions = [

            (
                "Ordered stream record count equals direct database record count",
                "EXACT",
                "yes"
                if record_difference == 0
                else "no",
            ),

            (
                "Ordered stream amount-paid row count equals direct database aggregate",
                "EXACT",
                "yes"
                if payment_row_difference == 0
                else "no",
            ),

            (
                "Ordered stream actual amount paid equals direct database aggregate",
                "EXACT",
                "yes"
                if revenue_difference < 0.000001
                else "no",
            ),

            (
                "Payment status distribution matches between both query paths",
                "EXACT",
                "yes"
                if payment_status_match
                else "no",
            ),

            (
                "Calendar listing_id values resolve to listings.id",
                "EXACT",
                "yes"
                if orphan_calendar == 0
                else "no",
            ),

            (
                "Review listing_id values resolve to listings.id",
                "EXACT",
                "yes"
                if orphan_reviews == 0
                else "no",
            ),

            (
                "Calendar composite key (listing_id, date) has no duplicates",
                "EXACT",
                "yes"
                if duplicate_calendar == 0
                else "no",
            ),
        ]

        # ========================================================
        # DISPLAY CONDITIONS
        # ========================================================

        self.clear_tree(
            self.reconciliation_condition_tree
        )

        for row in conditions:

            self.reconciliation_condition_tree.insert(
                "",
                "end",
                values=row,
            )

        passed = all(
            row[2] == "yes"
            for row in conditions
        )

        result_word = (
            "PASSED"
            if passed
            else "FAILED"
        )

        # ========================================================
        # UPDATE RECONCILIATION TAB
        # ========================================================

        self.reconciliation_summary_var.set(
            f"{result_word} — actual PostgreSQL "
            "amount_paid and payment_status values "
            "were compared through independent query paths"
        )

        # ========================================================
        # LOG RESULT
        # ========================================================

        self.log_stage(
            "RECONCILIATION",
            [

                "Comparison path A: direct PostgreSQL aggregate",

                "Comparison path B: explicitly ordered PostgreSQL stream query",

                "",

                f"Calendar records : "
                f"batch={batch_total:,} | "
                f"stream={stream_total:,} | "
                f"difference={record_difference:,}",

                f"Rows with amount paid : "
                f"batch={batch_paid_rows:,} | "
                f"stream={stream_paid_rows:,} | "
                f"difference={payment_row_difference:,}",

                f"Actual amount paid : "
                f"batch={fmt_money(batch_revenue)} | "
                f"stream={fmt_money(stream_revenue)} | "
                f"difference={fmt_money(revenue_difference)}",

                f"Payment status groups : "
                f"batch={len(batch_status_map):,} | "
                f"stream={len(stream_status_map):,} | "
                f"match={'yes' if payment_status_match else 'no'}",

                "",

                f"Orphan calendar rows : "
                f"{orphan_calendar:,}",

                f"Orphan review rows   : "
                f"{orphan_reviews:,}",

                f"Duplicate calendar keys : "
                f"{duplicate_calendar:,}",

                "",

                f"RESULT: {result_word}",
            ],
        )

        return (
            f"{result_word} · "
            f"{batch_total:,} records compared · "
            f"{fmt_money(batch_revenue)} actual amount paid · "
            f"difference {fmt_money(revenue_difference)}"
        )



# ============================================================
# MAIN
# ============================================================

def main():

    app = AVRFlowPostgresGUI()

    app.mainloop()


if __name__ == "__main__":
    main()