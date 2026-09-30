from pyspark.sql import functions as F

with_dims = (
    df_payments
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "inner")
    .select(
        "a.id",
        "a.order_id",
        "a.status",
        F.col("d.status").alias("order_status"),
        F.col("d.order_date"),
        F.col("d.amount").alias("order_amount"),
    )
)

result = (
    with_dims
    .groupBy(F.date_trunc("QUARTER", "order_date").alias("quarter_start"))
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("order_amount", [0.25, 0.5, 0.75]).alias("order_amount_quartiles"),
    )
    .filter(F.col("distinct_ids") > 1)
    .orderBy(F.col("quarter_start").desc())
)
