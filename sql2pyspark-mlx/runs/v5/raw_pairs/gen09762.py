from pyspark.sql import functions as F

remaining_keys = (
    df_shipments
    .select("carrier", "status")
    .subtract(
        df_shipments
        .filter(F.col("delivered_date").between("2023-06-05", "2024-11-17"))
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
    .groupBy(
        F.when(F.col("cost") >= 78, "PREMIUM").otherwise("STANDARD").alias("cost_tier"),
        F.date_trunc("YEAR", "delivered_date").alias("year_start"),
    )
    .agg(
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
        F.count_if(F.col("status") == "LOST").alias("lost_events"),
        F.stddev_samp("cost").alias("cost_stddev"),
    )
    .filter(F.col("lost_events") > 1)
)
