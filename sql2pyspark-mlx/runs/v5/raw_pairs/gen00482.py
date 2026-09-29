from pyspark.sql import functions as F

remaining_keys = (
    df_shipments
    .select("carrier", "status")
    .intersectAll(
        df_shipments
        .filter(F.col("status") == "IN_TRANSIT")
        .select("carrier", "status")
    )
)

reconciled = (
    df_shipments
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.carrier") == F.col("k.carrier")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(F.date_trunc("week", "shipped_date").alias("week_start"))
    .agg(
        F.percentile_approx("cost", 0.95).alias("cost_p95"),
        F.count("*").alias("cnt"),
        F.round(F.sum("cost") / F.nullif(F.count(F.when(F.col("status") == "LOST", 1)), F.lit(0)), 2).alias("cost_per_lost"),
        F.sum(F.when(F.col("carrier") == "FedEx", F.col("cost")).otherwise(0)).alias("fedex_cost"),
    )
    .filter(F.col("cost_per_lost") > 0)
)
