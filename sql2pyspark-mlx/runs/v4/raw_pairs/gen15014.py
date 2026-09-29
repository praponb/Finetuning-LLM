from pyspark.sql import functions as F

src = (
    df_orders
    .filter(F.col("customer_id").isin([35, 37]) & (F.col("order_date") >= "2023-10-01"))
)

orders_stats = (
    src
    .groupBy(F.year("order_date").alias("order_year"))
    .agg(
        F.count(F.when(F.col("channel") == "partner", 1)).alias("partner_count"),
        F.min("order_date").alias("first_order_date"),
    )
)

result = (
    orders_stats
    .filter(F.col("partner_count") < 6)
    .select("order_year", "partner_count", "first_order_date")
    .orderBy("partner_count")
)
