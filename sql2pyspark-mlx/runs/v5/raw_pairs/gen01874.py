from pyspark.sql import functions as F

matched_keys = (
    df_orders
    .select("id")
    .subtract(
        df_orders
        .filter(F.col("status") == "SHIPPED")
        .select("id")
    )
)

kept_rows = (
    df_orders
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(F.date_trunc("MONTH", "order_date").alias("activity_month"))
    .agg(
        F.sum(F.when(F.col("status") == "SHIPPED", F.col("amount")).otherwise(0)).alias("shipped_amount"),
        F.percentile_approx("shipping_fee", [0.25, 0.5, 0.75]).alias("shipping_fee_quartiles"),
        F.count_if(F.col("status") == "CANCELLED").alias("cancelled_events"),
    )
    .filter(F.col("cancelled_events") >= 1)
    .orderBy("activity_month")
)
