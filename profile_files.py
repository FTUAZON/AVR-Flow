"""Profile and validate the Seattle Airbnb files used by AVR-Flow."""
import json
import sys
import pandas as pd
import config as cfg

def profile_one(name: str):
    path = cfg.path_for(name)
    df = pd.read_csv(path)
    candidate_pks = [
        c for c in df.columns if df[c].notna().all() and df[c].is_unique
    ]
    return df, {
        "file": path.name,
        "role": cfg.FILES[name]["role"],
        "rows": int(len(df)),
        "columns": int(len(df.columns)),
        "column_names": df.columns.tolist(),
        "size_kb": round(path.stat().st_size / 1024, 1),
        "candidate_primary_keys": candidate_pks,
        "null_counts": {c: int(df[c].isna().sum()) for c in df.columns},
        "distinct_counts": {c: int(df[c].nunique(dropna=True)) for c in df.columns},
    }

def check_integrity(frames):
    listings = frames["listings"]
    ids = set(listings[cfg.LISTING_PRIMARY_KEY])
    checks = {}
    orphans = {}
    for name in ("calendar", "reviews"):
        checks[f"{name}.listing_id -> listings.id"] = bool(
            frames[name][cfg.PARTITION_KEY].isin(ids).all()
        )
        orphans[f"{name}_without_listing"] = int(
            (~frames[name][cfg.PARTITION_KEY].isin(ids)).sum()
        )
    return {"foreign_keys_resolve": checks, "orphan_counts": orphans}

def check_eligibility(frames, prof):
    qualifying = [n for n,p in prof.items() if p["role"] in ("Event","Entity")]
    one_to_many = []
    for name in ("calendar","reviews"):
        child = frames[name]
        if not child[cfg.PARTITION_KEY].is_unique:
            counts = child[cfg.PARTITION_KEY].value_counts()
            one_to_many.append({
                "parent":"listings", "child":name, "key":cfg.PARTITION_KEY,
                "children_min":int(counts.min()),
                "children_median":int(counts.median()),
                "children_max":int(counts.max()),
            })
    ts = pd.to_datetime(frames["calendar"][cfg.EVENT_TIME_FIELD], errors="coerce")
    dup = int(frames["calendar"].duplicated(
        [cfg.PARTITION_KEY, cfg.EVENT_TIME_FIELD]).sum())
    event_rows = sum(p["rows"] for p in prof.values() if p["role"] == "Event")
    return {
        "condition_1_three_related_files": {
            "met": len(qualifying) >= 3, "qualifying_files": qualifying},
        "condition_2_one_to_many": {
            "met": len(one_to_many) >= 1, "associations": one_to_many},
        "condition_3_timestamp": {
            "met": bool(ts.notna().all()),
            "field": f"calendar.{cfg.EVENT_TIME_FIELD}",
            "min": str(ts.min()), "max": str(ts.max()),
            "span_days": int((ts.max()-ts.min()).days) if ts.notna().all() else None},
        "condition_4_volume": {"met": event_rows >= 50000, "event_rows": event_rows},
        "calendar_composite_key": {
            "key":[cfg.PARTITION_KEY,cfg.EVENT_TIME_FIELD],
            "duplicates":dup, "unique":dup == 0},
    }

def main():
    cfg.banner("AVR-FLOW - FILE PROFILING")
    frames, prof = {}, {}
    for name in cfg.FILES:
        df,p = profile_one(name)
        frames[name],prof[name] = df,p
        print(f"{name:<12} {len(df):>10,} rows x {len(df.columns):>3} columns")
    integrity = check_integrity(frames)
    eligibility = check_eligibility(frames,prof)
    report = {"files":prof,"integrity":integrity,"eligibility":eligibility}
    cfg.OUT_PROFILE.write_text(json.dumps(report,indent=2,default=str),encoding="utf-8")
    cfg.banner("REFERENTIAL INTEGRITY")
    for k,v in integrity["foreign_keys_resolve"].items():
        print(f"{k}: {'PASS' if v else 'FAIL'}")
    cfg.banner("ELIGIBILITY")
    for k,v in eligibility.items():
        if isinstance(v,dict) and "met" in v:
            print(f"{k}: {'PASS' if v['met'] else 'FAIL'}")
    print(f"\nWrote {cfg.OUT_PROFILE}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
