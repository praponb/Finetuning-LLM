from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

with_last = (
    df_orders
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.store_id",
        "s.channel",
        "s.discount",
        F.col("c.unit_price"),
    )
)

result = (
    with_last
    .groupBy(
        F.when(F.col("discount") >= 0.41, "PREMIUM").otherwise("STANDARD").alias("discount_tier"),
        F.when(F.col("unit_price") >= 450, "HIGH").otherwise("LOW").alias("unit_price_tier"),
    )
    .agg(
        F.sum(F.when(F.col("channel") == "mobile", F.col("unit_price"))).alias("mobile_total_unit_price"),
        F.count("*").alias("event_count"),
        F.count_if(F.col("channel") == "mobile").alias("mobile_events"),
        F.count(F.when(F.col("channel") == "web", 1)).alias("web_rows"),
        F.sum("unit_price").alias("total_unit_price"),
    )
    .orderBy("total_unit_price")
)
