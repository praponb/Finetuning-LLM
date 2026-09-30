from pyspark.sql import functions as F

kept = (
    df_orders
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.unit_price") < 360), "left_anti")
)

result = (
    kept
    .groupBy(F.date_trunc("year", "order_date").alias("year_start"))
    .agg(
        F.size(F.collect_set("status")).alias("num_status"),
        F.sum(F.when(F.col("status") == "COMPLETED", F.col("amount"))).alias("completed_total_amount"),
        F.corr("discount", "amount").alias("corr_discount_amount"),
    )
    .filter(F.col("corr_discount_amount") < 1000000)
)
