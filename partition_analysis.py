"""Measure key-level and physical Spark partition skew for AVR-Flow."""
import csv,sys
import config as cfg
from parallel_compute import build_joined
from load_and_join import build_working_dataset

def key_level(working):
    counts=working[cfg.PARTITION_KEY].value_counts()
    return {"distinct_keys":int(counts.size),"min":int(counts.min()),
            "median":int(counts.median()),"max":int(counts.max()),
            "skew_ratio":round(counts.max()/counts.min(),2) if counts.min() else float("inf"),
            "counts":counts}

def partition_level(joined,partitions):
    sizes=joined.repartition(partitions,cfg.PARTITION_KEY).rdd.glom().map(len).collect()
    total=sum(sizes); even=total/partitions if partitions else 0
    mn,mx=(min(sizes),max(sizes)) if sizes else (0,0)
    return {"partitions":partitions,"sizes":sizes,"min":mn,"max":mx,
            "even_share":round(even,1),
            "skew_ratio":round(mx/mn,2) if mn else float("inf"),
            "worst_vs_even":round(mx/even,2) if even else 0.0}

def main():
    cfg.banner("AVR-FLOW - PARTITION ANALYSIS")
    working,_=build_working_dataset(verbose=False); kl=key_level(working)
    print(f"Distinct listing_id keys : {kl['distinct_keys']:,}")
    print(f"Records/key : min={kl['min']:,} median={kl['median']:,} max={kl['max']:,}")
    print(f"Key-level skew : {kl['skew_ratio']} : 1")
    spark=cfg.build_spark(); rows=[]
    try:
        joined,_=build_joined(spark,verbose=False)
        for p in cfg.PARTITION_SETTINGS:
            r=partition_level(joined,p)
            print(f"physical partitions={p}: min={r['min']:,} max={r['max']:,} "
                  f"skew={r['skew_ratio']}:1 worst/even={r['worst_vs_even']}x")
            for i,size in enumerate(r["sizes"]):
                rows.append({"level":"physical_partition","setting":p,"identifier":i,
                             "record_count":size,"even_share":r["even_share"],
                             "vs_even":round(size/r["even_share"],3) if r["even_share"] else 0})
    finally: spark.stop()
    even_key=len(working)/kl["distinct_keys"] if kl["distinct_keys"] else 0
    for key,count in kl["counts"].items():
        rows.append({"level":"partition_key","setting":cfg.PARTITION_KEY,
                     "identifier":key,"record_count":int(count),
                     "even_share":round(even_key,1),
                     "vs_even":round(count/even_key,3) if even_key else 0})
    fields=["level","setting","identifier","record_count","even_share","vs_even"]
    with open(cfg.OUT_PARTITIONS,"w",newline="",encoding="utf-8") as fh:
        w=csv.DictWriter(fh,fieldnames=fields); w.writeheader(); w.writerows(rows)
    print(f"\nWrote {cfg.OUT_PARTITIONS}")
    return 0

if __name__=="__main__": sys.exit(main())
