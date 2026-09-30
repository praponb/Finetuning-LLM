from pyspark.sql import functions as F

eligible = (
    df_orders
    .alias("s")
    .join(df_payments.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.paid_at") >= F.col("s.order_date")), "left_anti")
)

result = (
    eligible
    .groupBy(F.date_trunc("QUARTER", "order_date").alias("quarter_start"))
    .agg(
        F.count(F.when(F.col("status") == "CANCELLED", 1)).alias("cancelled_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
    .orderBy(F.col("cancelled_count").asc_nulls_last())
)
