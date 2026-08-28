"""Evaluate partition-key candidates for AVR-Flow."""
import json,sys
import config as cfg
from load_and_join import build_working_dataset,load_frames

NEVER_PARTITION={"id",cfg.PRICE_FIELD,cfg.EVENT_TIME_FIELD,cfg.AVAILABILITY_FIELD,
                 "comments","name","description"}

def owning_file(column,frames):
    owners=[name for name,df in frames.items() if column in df.columns]
    return " + ".join(owners) if owners else "derived"

def evaluate(working,frames):
    rows=len(working); candidates=[]
    for col in working.columns:
        counts=working[col].value_counts(dropna=True); distinct=int(counts.size); rejects=[]
        if col in NEVER_PARTITION: rejects.append("identifier, timestamp, availability, or measure")
        if distinct<2: rejects.append(f"only {distinct} distinct value(s)")
        if rows and distinct>rows*0.5: rejects.append(f"{distinct:,} distinct values is near-unique")
        skew=round(counts.max()/counts.min(),2) if distinct and counts.min()>0 else float("inf")
        candidates.append({"column":col,"source_file":owning_file(col,frames),
            "distinct":distinct,"min":int(counts.min()) if distinct else 0,
            "median":int(counts.median()) if distinct else 0,
            "max":int(counts.max()) if distinct else 0,"skew_ratio":skew,
            "viable":not rejects,"rejected_because":"; ".join(rejects)})
    return candidates

def main():
    cfg.banner("AVR-FLOW - PARTITIONING STRATEGY")
    frames=load_frames(); working,join_report=build_working_dataset(frames,verbose=False)
    candidates=evaluate(working,frames)
    for c in sorted(candidates,key=lambda x:(not x["viable"],-x["skew_ratio"])):
        verdict="VIABLE" if c["viable"] else c["rejected_because"]
        print(f"{c['column']:<28} distinct={c['distinct']:>7,} min={c['min']:>6,} "
              f"median={c['median']:>6,} max={c['max']:>6,} skew={c['skew_ratio']:>7.2f} {verdict}")
    chosen=next(c for c in candidates if c["column"]==cfg.PARTITION_KEY)
    workload=(f"Compute observed nights, occupied nights, occupancy rate, estimated "
              f"booked-night revenue, and average booked price per {cfg.PARTITION_KEY}.")
    output={"chosen_key":cfg.PARTITION_KEY,
            "reason":"Listing-owned key survives the calendar-listing join and partitions calendar events by listing.",
            "prediction":chosen,"all_candidates":candidates,"workload":workload,"join":join_report}
    cfg.OUT_PARTITION_STRATEGY.write_text(json.dumps(output,indent=2),encoding="utf-8")
    print(f"\nWrote {cfg.OUT_PARTITION_STRATEGY}")
    return 0

if __name__=="__main__": sys.exit(main())
