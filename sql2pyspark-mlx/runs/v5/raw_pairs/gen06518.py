from pyspark.sql import functions as F

kept_keys = (
    df_shipments
    .select("id", "status")
    .subtract(
        df_shipments
        .filter(F.col("carrier").isin(["DHL", "Kerry", "UPS"]))
        .select("id", "status")
    )
)

reconciled = (
    df_shipments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy("status")
    .agg(
        F.percentile_approx("cost", 0.5).alias("cost_p50"),
        F.count(F.when(F.col("carrier") == "DHL", 1)).alias("dhl_rows"),
        F.avg(F.when(F.col("status") == "RETURNED", F.col("cost"))).alias("returned_avg_cost"),
    )
)
