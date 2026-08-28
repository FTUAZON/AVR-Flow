"""Benchmark AVR-Flow sequential vs bounded Spark parallelism."""
import csv,statistics,sys,time
import config as cfg
from parallel_compute import build_joined,compute_parallel
from sequential_baseline import run_baseline
from load_and_join import build_working_dataset

def bench_one(joined,partitions,repeats):
    times=[]; groups=None
    for _ in range(repeats):
        start=time.perf_counter(); _,result=compute_parallel(joined,partitions)
        groups=result.count(); times.append(time.perf_counter()-start)
    return {"partitions":partitions,"runs":[round(t,4) for t in times],
            "median":round(statistics.median(times),4),"min":round(min(times),4),
            "max":round(max(times),4),"groups":groups}

def main():
    cfg.banner("AVR-FLOW - BENCHMARK")
    working,_=build_working_dataset(verbose=False)
    _,baseline=run_baseline(working,verbose=False)
    spark=cfg.build_spark()
    try:
        joined,spark_join=build_joined(spark,verbose=False)
        results=[]
        for p in cfg.PARTITION_SETTINGS:
            r=bench_one(joined,p,cfg.BENCHMARK_REPEATS); results.append(r)
            print(f"partitions={p:>2} | median={r['median']:.4f} s | groups={r['groups']}")
        expected=baseline["groups"]
        assert all(r["groups"]==expected for r in results)
        base=baseline["median_seconds"]
        rows=[{"run":"Sequential baseline","parallelism_partitions":"1",
               "execution_time_s":f"{base:.4f}","groups":expected,
               "speedup_vs_baseline":"1.0000","correct":"Yes"}]
        for r in results:
            rows.append({"run":f"Parallel ({r['partitions']} partitions)",
                         "parallelism_partitions":r["partitions"],
                         "execution_time_s":f"{r['median']:.4f}",
                         "groups":r["groups"],
                         "speedup_vs_baseline":f"{base/r['median']:.4f}",
                         "correct":"Yes"})
        with open(cfg.OUT_BENCHMARK,"w",newline="",encoding="utf-8") as fh:
            writer=csv.DictWriter(fh,fieldnames=rows[0].keys()); writer.writeheader(); writer.writerows(rows)
        print(f"\nWrote {cfg.OUT_BENCHMARK}")
        print(f"BroadcastHashJoin: {spark_join['broadcast_hash_joins']}")
        print(f"SortMergeJoin: {spark_join['sort_merge_joins']}")
    finally: spark.stop()
    return 0

if __name__=="__main__": sys.exit(main())
