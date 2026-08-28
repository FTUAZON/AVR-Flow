"""Render polished, top-to-bottom AVR-Flow diagrams using Graphviz.

The diagrams are organized from SOURCE -> PROCESSING -> RESULT so that a
reader can follow the AVR-Flow research pipeline from top to bottom.
"""

import shutil
import subprocess
import sys
from pathlib import Path

import config as cfg


# ============================================================
# AVR-FLOW COLOR PALETTE
# ============================================================

NAVY = "#17324D"
BLUE = "#2F6690"
TEAL = "#3FA7A3"
PURPLE = "#6C5B9A"
GREEN = "#4F8A5B"
GOLD = "#D9A441"

LIGHT_BLUE = "#EAF3F8"
LIGHT_TEAL = "#E8F5F4"
LIGHT_PURPLE = "#F1EEFA"
LIGHT_GREEN = "#ECF5ED"
LIGHT_GOLD = "#FFF5DC"
LIGHT_GRAY = "#F3F5F7"

DARK = "#1F2937"
GRAY = "#5B6573"


# ============================================================
# ENTITY / UML MODEL
# ============================================================

ENTITY = r"""
digraph EntityModel {

    graph [
        rankdir=TB,
        bgcolor="white",
        pad="0.40",
        nodesep="0.75",
        ranksep="0.75",
        splines=ortho,
        fontname="Helvetica",
        label="AVR-FLOW\nENTITY & RELATIONSHIP MODEL",
        labelloc="t",
        labeljust="c",
        fontsize="25",
        fontcolor="#17324D"
    ];

    node [
        shape=plain,
        fontname="Helvetica",
        fontcolor="#1F2937"
    ];

    edge [
        color="#6B7280",
        penwidth="1.8",
        arrowsize="0.8",
        fontname="Helvetica",
        fontsize="11",
        fontcolor="#5B6573"
    ];

    Listing [
        label=<
            <TABLE BORDER="0" CELLBORDER="0" CELLSPACING="0"
                   CELLPADDING="0" BGCOLOR="#EAF3F8">
                <TR>
                    <TD BGCOLOR="#17324D" CELLPADDING="14">
                        <FONT COLOR="white" POINT-SIZE="17">
                            <B>LISTING</B>
                        </FONT>
                    </TD>
                </TR>
                <TR>
                    <TD ALIGN="LEFT" CELLPADDING="9">
                        <FONT COLOR="#2F6690" POINT-SIZE="12">
                            <B>ENTITY</B>
                        </FONT>
                    </TD>
                </TR>
                <TR><TD ALIGN="LEFT" CELLPADDING="6"><B>PK</B> id</TD></TR>
                <TR><TD ALIGN="LEFT" CELLPADDING="6">property_type</TD></TR>
                <TR><TD ALIGN="LEFT" CELLPADDING="6">room_type</TD></TR>
                <TR><TD ALIGN="LEFT" CELLPADDING="6">neighbourhood</TD></TR>
                <TR><TD ALIGN="LEFT" CELLPADDING="6">accommodates</TD></TR>
                <TR><TD ALIGN="LEFT" CELLPADDING="6">review_scores_rating</TD></TR>
            </TABLE>
        >
    ];

    Calendar [
        label=<
            <TABLE BORDER="0" CELLBORDER="0" CELLSPACING="0"
                   CELLPADDING="0" BGCOLOR="#E8F5F4">
                <TR>
                    <TD BGCOLOR="#3FA7A3" CELLPADDING="14">
                        <FONT COLOR="white" POINT-SIZE="17">
                            <B>CALENDAR DAY</B>
                        </FONT>
                    </TD>
                </TR>
                <TR>
                    <TD ALIGN="LEFT" CELLPADDING="9">
                        <FONT COLOR="#3FA7A3" POINT-SIZE="12">
                            <B>EVENT</B>
                        </FONT>
                    </TD>
                </TR>
                <TR><TD ALIGN="LEFT" CELLPADDING="6"><B>PK</B> (listing_id, date)</TD></TR>
                <TR><TD ALIGN="LEFT" CELLPADDING="6"><B>FK</B> listing_id</TD></TR>
                <TR><TD ALIGN="LEFT" CELLPADDING="6">available</TD></TR>
                <TR><TD ALIGN="LEFT" CELLPADDING="6">price</TD></TR>
                <TR>
                    <TD ALIGN="LEFT" CELLPADDING="6">
                        <FONT COLOR="#3FA7A3"><B>eventTime</B> date</FONT>
                    </TD>
                </TR>
                <TR>
                    <TD ALIGN="LEFT" CELLPADDING="6">
                        <FONT COLOR="#3FA7A3"><B>partitionKey</B> listing_id</FONT>
                    </TD>
                </TR>
            </TABLE>
        >
    ];

    Review [
        label=<
            <TABLE BORDER="0" CELLBORDER="0" CELLSPACING="0"
                   CELLPADDING="0" BGCOLOR="#F1EEFA">
                <TR>
                    <TD BGCOLOR="#6C5B9A" CELLPADDING="14">
                        <FONT COLOR="white" POINT-SIZE="17">
                            <B>REVIEW</B>
                        </FONT>
                    </TD>
                </TR>
                <TR>
                    <TD ALIGN="LEFT" CELLPADDING="9">
                        <FONT COLOR="#6C5B9A" POINT-SIZE="12">
                            <B>EVENT</B>
                        </FONT>
                    </TD>
                </TR>
                <TR><TD ALIGN="LEFT" CELLPADDING="6"><B>PK</B> id</TD></TR>
                <TR><TD ALIGN="LEFT" CELLPADDING="6"><B>FK</B> listing_id</TD></TR>
                <TR><TD ALIGN="LEFT" CELLPADDING="6">date</TD></TR>
                <TR><TD ALIGN="LEFT" CELLPADDING="6">reviewer_id</TD></TR>
                <TR><TD ALIGN="LEFT" CELLPADDING="6">comments</TD></TR>
            </TABLE>
        >
    ];

    { rank=same; Calendar; Review; }

    Listing -> Calendar [
        label="  1  :  0..*  ",
        color="#2F6690",
        fontcolor="#2F6690",
        penwidth="2.3"
    ];

    Listing -> Review [
        label="  1  :  0..*  ",
        color="#6C5B9A",
        fontcolor="#6C5B9A",
        penwidth="2.3"
    ];
}
"""


# ============================================================
# AVR-FLOW ARCHITECTURE / PROCESSING PIPELINE
# ============================================================

ARCHITECTURE = r"""
digraph AVRFlow {

    graph [
        rankdir=TB,
        bgcolor="white",
        pad="0.40",
        nodesep="0.55",
        ranksep="0.65",
        splines=ortho,
        fontname="Helvetica",
        label="AVR-FLOW\nPARTITIONED PARALLEL & DISTRIBUTED OCCUPANCY REVENUE INTELLIGENCE PLATFORM",
        labelloc="t",
        labeljust="c",
        fontsize="23",
        fontcolor="#17324D"
    ];

    node [
        shape=box,
        style="rounded,filled",
        fontname="Helvetica",
        fontsize="11",
        margin="0.18,0.12",
        color="#CBD5E1",
        penwidth="1.5",
        fontcolor="#1F2937"
    ];

    edge [
        color="#7B8794",
        penwidth="1.7",
        arrowsize="0.75",
        fontname="Helvetica",
        fontsize="10",
        fontcolor="#5B6573"
    ];


    // ========================================================
    // 1. SOURCE DATA
    // ========================================================

    calendar [
        label="calendar.csv\n~1.39M daily events\nlisting_id • date • available • price",
        fillcolor="#E8F5F4",
        color="#3FA7A3"
    ];

    listings [
        label="listings.csv\nListing entity\nid • host • property • room attributes",
        fillcolor="#EAF3F8",
        color="#2F6690"
    ];

    reviews [
        label="reviews.csv\nReview events\nid • listing_id • date • reviewer • comments",
        fillcolor="#F1EEFA",
        color="#6C5B9A"
    ];

    { rank=same; calendar; listings; reviews; }


    // ========================================================
    // 2. PROFILE & VALIDATION
    // ========================================================

    profile [
        label="1. PROFILE & VALIDATE\nprofile_files.py\nValidate schema • primary keys • foreign keys • dates • data volume",
        fillcolor="#F3F5F7",
        color="#5B6573",
        penwidth="2.0"
    ];


    // ========================================================
    // 3. BUILD WORKING DATASET
    // ========================================================

    join [
        label="2. BUILD WORKING DATASET\nload_and_join.py\ncalendar.csv + listings.csv\nMany-to-one join on listing_id\nNO review fan-out",
        fillcolor="#EAF3F8",
        color="#2F6690",
        penwidth="2.0"
    ];


    // ========================================================
    // REVIEW SUPPORT PROCESS
    // ========================================================

    reviewagg [
        label="Review Aggregation\nreview data grouped by listing_id\nSupporting intelligence feature",
        fillcolor="#F1EEFA",
        color="#6C5B9A",
        fontsize="10"
    ];


    // ========================================================
    // 4. PARTITIONING
    // ========================================================

    partition [
        label="3. PARTITION BY listing_id\nBounded repartition\nPartition key carried through processing",
        fillcolor="#FFF5DC",
        color="#D9A441",
        penwidth="2.4"
    ];


    // ========================================================
    // 5. PARALLEL PROCESSING
    // ========================================================

    workers [
        label="4. PARALLEL SPARK PROCESSING\nWorker 1     Worker 2     Worker 3     …     Worker N\nEach worker processes a partition of listing_id",
        fillcolor="#ECF5ED",
        color="#4F8A5B",
        penwidth="2.4",
        fontsize="12"
    ];


    // ========================================================
    // 6. AGGREGATION
    // ========================================================

    aggregate [
        label="5. OCCUPANCY + REVENUE AGGREGATION\nObserved nights • Occupied nights • Occupancy rate\nEstimated booked revenue • Average booked price",
        fillcolor="#E8F5F4",
        color="#3FA7A3",
        penwidth="2.4",
        fontsize="12"
    ];


    // ========================================================
    // 7. BENCHMARKING / ANALYSIS
    // ========================================================

    analysis [
        label="6. BENCHMARKING & PARTITION ANALYSIS\nsequential_baseline.py  →  Pandas reference\nbenchmark.py            →  Compare 2 / 4 / 8 partitions\npartition_analysis.py   →  Key and physical skew\npartition_strategy.py   →  Justify listing_id",
        fillcolor="#FFF5DC",
        color="#D9A441",
        penwidth="2.0"
    ];


    // ========================================================
    // 8. INTELLIGENCE
    // ========================================================

    intelligence [
        label="7. OCCUPANCY & REVENUE INTELLIGENCE\nCombine computed occupancy/revenue indicators\nwith listing attributes and review-derived features",
        fillcolor="#EAF3F8",
        color="#17324D",
        penwidth="2.6",
        fontsize="12"
    ];


    // ========================================================
    // 9. DYNAMIC PRICING RESULT
    // ========================================================

    pricing [
        label="8. DYNAMIC PRICING MODEL\nRecommended price / pricing decision\nBased on occupancy, revenue, listing context, and analytical results",
        fillcolor="#ECF5ED",
        color="#4F8A5B",
        penwidth="2.8",
        fontsize="13"
    ];


    // ========================================================
    // MAIN TOP-TO-BOTTOM PIPELINE
    // ========================================================

    calendar -> profile;
    listings -> profile;
    reviews -> profile;

    profile -> join;

    calendar -> join;
    listings -> join;

    join -> partition [
        color="#D9A441",
        penwidth="2.4"
    ];

    partition -> workers [
        color="#4F8A5B",
        penwidth="2.4"
    ];

    workers -> aggregate [
        color="#3FA7A3",
        penwidth="2.4"
    ];

    aggregate -> analysis [
        color="#D9A441",
        penwidth="2.1"
    ];

    analysis -> intelligence [
        color="#17324D",
        penwidth="2.2"
    ];

    intelligence -> pricing [
        color="#4F8A5B",
        penwidth="2.8"
    ];


    // ========================================================
    // SUPPORTING REVIEW PATH
    // ========================================================

    reviews -> reviewagg [
        color="#6C5B9A",
        penwidth="1.8",
        constraint=false
    ];

    reviewagg -> intelligence [
        color="#6C5B9A",
        penwidth="1.8",
        style="dashed",
        label="supporting review features",
        constraint=false
    ];
}
"""


# ============================================================
# GRAPHVIZ CHECK
# ============================================================

def check_graphviz():
    """Return the Graphviz dot executable if installed."""

    dot = shutil.which("dot")

    if dot:
        return dot

    print()
    print("ERROR: Graphviz 'dot' was not found on PATH.")
    print()
    print("Check your Graphviz installation with:")
    print()
    print("    dot -V")
    print()

    return None


# ============================================================
# RENDER FUNCTION
# ============================================================

def render(dot_source, output_png, diagram_name):

    dot = check_graphviz()

    if not dot:
        return False

    output_png = Path(output_png)
    output_png.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    dot_file = output_png.with_suffix(".dot")

    dot_file.write_text(
        dot_source,
        encoding="utf-8"
    )

    try:

        subprocess.run(
            [
                dot,
                "-Tpng",
                str(dot_file),
                "-o",
                str(output_png)
            ],
            check=True,
            capture_output=True,
            text=True
        )

    except subprocess.CalledProcessError as exc:

        print()
        print(f"ERROR rendering {diagram_name}:")
        print(
            exc.stderr
            or exc.stdout
            or "Unknown Graphviz error."
        )

        return False

    finally:

        if dot_file.exists():
            dot_file.unlink()

    print(f"✓ Wrote {output_png}")

    return True


# ============================================================
# MAIN
# ============================================================

def main():

    cfg.banner(
        "AVR-FLOW - RENDER TOP-TO-BOTTOM DIAGRAMS"
    )

    entity_output = (
        cfg.DOCS_DIR
        / "avr_flow_entity_model.png"
    )

    architecture_output = (
        cfg.ARCH_DIR
        / "avr_flow_architecture.png"
    )

    entity_ok = render(
        ENTITY,
        entity_output,
        "AVR-Flow Entity Model"
    )

    architecture_ok = render(
        ARCHITECTURE,
        architecture_output,
        "AVR-Flow Architecture"
    )

    print()

    if entity_ok:
        print(
            f"Entity diagram : {entity_output}"
        )

    if architecture_ok:
        print(
            f"Architecture diagram : {architecture_output}"
        )

    if entity_ok and architecture_ok:

        print()
        print(
            "✓ AVR-Flow diagrams rendered successfully."
        )
        print(
            "✓ Reading direction: SOURCE → PROCESSING → RESULT"
        )

        return 0

    return 1


if __name__ == "__main__":
    sys.exit(main())