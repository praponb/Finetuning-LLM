from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel")
w2 = Window.orderBy("discount", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("customer_id")
w4 = Window.partitionBy("store_id")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

with_windows = (
    df_orders
    .filter(F.col("store_id").isin([19, 36]))
    .withColumn("corr_shipping_fee_amount", F.corr("shipping_fee", "amount").over(w))
    .withColumn("discount_avg_3", F.avg("discount").over(w2))
)

scored = (
    with_windows
    .withColumn("customer_id_avg_discount_avg_3", F.avg("discount_avg_3").over(w3))
    .withColumn("corr_shipping_fee_discount", F.corr("shipping_fee", "discount").over(w4))
)

eligible = (
    scored
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.product_id").isNotNull()), "left_anti")
)

latest_match = (
    eligible
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.order_date",
        "s.channel",
        "s.discount",
        "s.discount_avg_3",
        "s.customer_id_avg_discount_avg_3",
        F.col("c.amount").alias("payment_amount"),
    )
)

result = (
    latest_match
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "inner")
    .rollup(F.col("c.country").alias("country"), F.col("src.channel").alias("channel"))
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("src.discount_avg_3")).alias("min_discount_avg_3"),
        F.sum(F.col("src.customer_id_avg_discount_avg_3")).alias("total_customer_id_avg_discount_avg_3"),
    )
)
