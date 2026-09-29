from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("order_date").desc(), "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")
w_ordered = Window.partitionBy("order_id").orderBy("dedup_rank", "id")
w4 = Window.partitionBy("channel")
w5 = Window.partitionBy("order_id")
w6 = Window.partitionBy("product_id").orderBy("amount", "id").rowsBetween(-2, Window.currentRow)

with_dims = (
    df_order_items
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.amount"), F.col("d.order_date"), F.col("d.channel"))
)

deduped = (
    with_dims
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

ranked = (
    deduped
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

with_dims_order_items = (
    ranked
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.product_name"))
)

result = (
    with_dims_order_items
    .select(
        "*",
        F.lead("unit_price", 1).over(w_ordered).alias("next_unit_price"),
        F.round((F.col("unit_price") / F.sum("unit_price").over(w4)), 4).alias("unit_price_share"),
        F.corr("quantity", "unit_price").over(w5).alias("corr_quantity_unit_price"),
        F.avg("unit_price").over(w6).alias("unit_price_avg_2"),
    )
)
