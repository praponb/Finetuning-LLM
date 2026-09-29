from pyspark.sql import functions as F

remaining_keys = (
    df_shipments
    .select("status")
    .subtract(
        df_shipments
        .filter(F.col("shipped_date") < "2025-05-09")
        .select("status")
    )
)

kept_rows = (
    df_shipments
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy("order_id")
    .agg(
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
        F.count(F.when(F.col("status") == "DELIVERED", 1)).alias("delivered_rows"),
        F.countDistinct("carrier").alias("distinct_carriers"),
        F.stddev_samp("cost").alias("cost_stddev"),
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles_2"),
    )
)
