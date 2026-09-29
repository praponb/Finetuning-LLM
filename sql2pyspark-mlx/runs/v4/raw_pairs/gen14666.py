from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("order_id").orderBy("unit_price", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("order_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("order_id").orderBy(F.col("unit_price_whole").desc(), "id")
w5 = Window.partitionBy("order_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("unit_price", "id")
w7 = Window.partitionBy("order_id").orderBy("first_known_unit_price_whole", "id")
w8 = Window.partitionBy("order_id").orderBy("unit_price_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("unit_price_whole", "id")
w10 = Window.partitionBy("order_id").orderBy("discount", "id").rowsBetween(-3, -1)
w11 = Window.partitionBy("order_id")

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & (F.col("product_id") == 10))
    .select("*", (F.col("quantity") * F.col("unit_price")).alias("line_total"))
)

ranked = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

base = (
    ranked
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "unit_price",
        F.ceil("unit_price").alias("unit_price_whole"),
    )
)

metrics = (
    base
    .select(
        "*",
        F.avg("unit_price_whole").over(w2).alias("unit_price_whole_avg_3"),
        F.first("unit_price_whole", ignorenulls=True).over(w3).alias("first_known_unit_price_whole"),
    )
)

valid_order_items = (
    metrics
    .filter(F.col("unit_price_whole_avg_3") >= 56)
    .select(
        "id",
        "order_id",
        "unit_price",
        "unit_price_whole",
        "first_known_unit_price_whole",
        F.when(F.col("first_known_unit_price_whole") >= 83, "High").when(F.col("first_known_unit_price_whole") >= 15, "Medium").otherwise("Low").alias("first_known_unit_price_whole_level"),
    )
)

enriched = (
    valid_order_items
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "inner")
    .select("src.*", F.col("o.amount"), F.col("o.discount"), F.col("o.shipping_fee"))
)

enriched_order_items = (
    enriched
    .withColumn("unit_price_whole_bucket", F.ntile(3).over(w4))
    .withColumn("cumulative_first_known_unit_price_whole", F.sum("first_known_unit_price_whole").over(w5))
    .withColumn("seq_no", F.row_number().over(w6))
    .withColumn("prev_first_known_unit_price_whole", F.lag("first_known_unit_price_whole", 1, 0).over(w7))
    .withColumn("last_known_unit_price", F.last("unit_price", ignorenulls=True).over(w8))
)

result = (
    enriched_order_items
    .withColumn("unit_price_whole_bucket_2", F.ntile(4).over(w9))
    .withColumn("discount_avg_3_prior", F.avg("discount").over(w10))
    .withColumn("order_id_avg_unit_price", F.avg("unit_price").over(w11))
)
