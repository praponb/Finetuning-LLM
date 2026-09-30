from pyspark.sql import functions as F

kept_keys = (
    df_shipments
    .select("order_id")
    .exceptAll(
        df_shipments
        .filter(F.col("cost") <= 35)
        .select("order_id")
    )
)

reconciled = (
    df_shipments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy("order_id", "status")
    .agg(
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_count"),
        F.stddev_samp("cost").alias("cost_stddev"),
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
    )
)
