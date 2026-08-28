"""Sequential pandas reference for AVR-Flow."""
import statistics
import sys
import time
import pandas as pd
import config as cfg
from load_and_join import build_working_dataset

def compute(working):
    def booked_mean(s):
        return s[working.loc[s.index,"occupied_proxy"]].mean()
    result=(working.groupby(cfg.PARTITION_KEY).agg(
        observed_nights=(cfg.EVENT_TIME_FIELD,"count"),
        occupied_nights=("occupied_proxy","sum"),
        estimated_revenue=("estimated_booked_revenue","sum"),
        average_booked_price=(cfg.PRICE_FIELD,booked_mean),
    ).reset_index())
    result["occupancy_rate"]=result["occupied_nights"]/result["observed_nights"]
    return result[[cfg.PARTITION_KEY,"observed_nights","occupied_nights",
                   "occupancy_rate","estimated_revenue","average_booked_price"]]

def run_baseline(working=None,repeats=cfg.BASELINE_REPEATS,verbose=True):
    if working is None:
        working,_=build_working_dataset(verbose=False)
    times=[]; result=None
    for _ in range(repeats):
        start=time.perf_counter(); result=compute(working)
        times.append(time.perf_counter()-start)
    report={"runs":[round(t,4) for t in times],
            "median_seconds":round(statistics.median(times),4),
            "mean_seconds":round(statistics.fmean(times),4),
            "groups":int(len(result)),"repeats":repeats}
    if verbose:
        cfg.banner("SEQUENTIAL BASELINE")
        for i,t in enumerate(times,1): print(f"run {i}: {t:.4f} s")
        print(f"median : {report['median_seconds']:.4f} s")
        print(f"groups : {report['groups']}")
    return result,report

def main():
    cfg.banner("AVR-FLOW - SEQUENTIAL BASELINE")
    working,_=build_working_dataset()
    result,_=run_baseline(working)
    result.to_csv(cfg.OUT_BASELINE,index=False)
    print(f"\nWrote {cfg.OUT_BASELINE}")
    return 0

if __name__=="__main__":
    sys.exit(main())
