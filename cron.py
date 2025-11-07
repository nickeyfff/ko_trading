import time

from calculate.calc_indicator import run_indicator_calculate
from database import csindex
from database.index import run_csindex_update
from database.shenwan import run_shenwan_industry_update

if __name__ == "__main__":
    start = time.time()

    run_csindex_update()
    run_shenwan_industry_update()

    symbols = [s for s in csindex.query("ChinaA")["symbol"]]
    run_indicator_calculate(symbols=symbols)
    # run_reversal_analysis(symbols=symbols)
    end = time.time()
    print("执行时间: {:.6f} 秒".format(end - start))
