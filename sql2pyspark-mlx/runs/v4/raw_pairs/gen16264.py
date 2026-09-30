from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isNotNull() & (F.col("amount") > 770) & (F.col("shipping_fee") >= 36))
    .groupBy(
        F.date_trunc("MONTH", "order_date").alias("order_month"),
        "channel",
        F.year("order_date").alias("order_year"),
    )
    .agg(F.count(F.when(F.col("channel") == "partner", 1)).alias("partner_count"))
)
