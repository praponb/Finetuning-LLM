from pyspark.sql import functions as F

remaining_keys = (
    df_orders
    .select("status")
    .intersectAll(
        df_orders
        .filter(F.col("discount") > 0.11)
        .select("status")
    )
)

reconciled = (
    df_orders
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .withColumn("activity_month", F.date_trunc("month", "order_date"))
    .withColumn("year_start", F.date_trunc("year", "order_date"))
    .groupingSets(
        [["activity_month", "year_start", "customer_id"], ["activity_month", "year_start"], []],
        "activity_month",
        "year_start",
        "customer_id",
    )
    .agg(
        F.sum(F.when(F.col("status") == "CANCELLED", F.col("amount"))).alias("cancelled_total_amount"),
        F.percentile_approx("discount", [0.25, 0.5, 0.75]).alias("discount_quartiles"),
        F.grouping("activity_month").alias("grouping_id"),
    )
)
