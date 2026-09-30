from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel")
w_running = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_orders
    .filter((F.col("order_date") > F.to_date(F.lit("2023-11-20"))) & F.col("channel").isNotNull() & F.col("channel").isin(["web", "partner"]))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.when(F.col("status").isin(["SHIPPED", "RETURNED"]), "Priority").otherwise("Other").alias("status_group"),
        (F.col("amount") / F.nullif("discount", F.lit(0))).alias("amount_per_discount"),
    )
)

result = (
    valid
    .withColumn("channel_stddev_samp_shipping_fee", F.stddev_samp("shipping_fee").over(w))
    .withColumn("first_known_shipping_fee", F.first("shipping_fee", ignorenulls=True).over(w_running))
)
