from pyspark.sql import functions as F

kept_keys = (
    df_payments
    .select("status", "method")
    .intersectAll(
        df_payments
        .filter(F.col("order_id").isNotNull())
        .select("status", "method")
    )
)

kept_rows = (
    df_payments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.method") == F.col("k.method")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .withColumn("amount_tier", F.when(F.col("amount") >= 1610, "LARGE").otherwise("SMALL"))
    .withColumn("activity_month", F.date_trunc("MONTH", "paid_at"))
    .rollup("amount_tier", "activity_month")
    .agg(
        F.count("*").alias("cnt"),
        F.min("paid_at").alias("started_at"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("status") == "SUCCESS", 1)), F.lit(0)), 2).alias("amount_per_success"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("method") == "card", 1)), F.lit(0)), 2).alias("amount_per_card"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("cnt").asc_nulls_first())
)
