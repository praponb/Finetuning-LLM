from pyspark.sql import functions as F

kept = (
    df_orders
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.product_id").isNotNull()), "left_anti")
)

result = (
    kept
    .groupBy(F.date_trunc("quarter", "order_date").alias("quarter_start"))
    .agg(
        F.count("*").alias("num_rows"),
        F.avg("discount").alias("avg_discount"),
        F.count("*").alias("row_count"),
    )
)
