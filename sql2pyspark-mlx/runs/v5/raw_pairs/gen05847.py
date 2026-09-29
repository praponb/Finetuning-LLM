from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("line_total", "id")
w2 = Window.partitionBy("order_id").orderBy(F.col("latest_rank").desc(), "id")
w3 = Window.partitionBy("product_id").orderBy(F.col("order_date").desc_nulls_first(), "id")
quantity_by_product_id = df_order_items.groupBy("product_id").agg(F.avg("quantity").alias("avg_order_item_quantity"))

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select("*", (F.col("quantity") * F.col("unit_price")).alias("line_total"))
)

ranked = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

joined = (
    ranked
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id") == F.col("b.id"), "inner")
    .select(
        "s.id",
        "s.order_id",
        "s.product_id",
        "s.quantity",
        "s.latest_rank",
        F.col("b.order_date"),
        F.col("b.shipping_fee"),
    )
)

latest = (
    joined
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

ranked_order_items = (
    latest
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "product_id", "latest_rank", "order_date")
)

base = (
    ranked_order_items
    .filter(F.col("product_id").isin([14, 17]))
    .withColumn("order_date_plus_3d", F.date_add("order_date", 3))
)

result = (
    base
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "latest_rank",
        "order_date_plus_3d",
        F.col("avg_order_item_quantity"),
    )
    .orderBy(F.col("product_id").desc_nulls_first())
)
