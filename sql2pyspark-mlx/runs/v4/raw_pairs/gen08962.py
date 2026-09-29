from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("unit_price").desc(), "id")
w2 = Window.partitionBy("product_id")
w3 = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(-2, -1)
w4 = Window.partitionBy("product_id", "order_id").orderBy(F.col("quantity").desc(), "id")
w5 = Window.partitionBy("order_id", "product_id").orderBy(F.col("price").desc(), "id")
w_hist = Window.partitionBy("order_id")
w_recent = Window.partitionBy("order_id").orderBy("quantity")

scored = (
    df_order_items
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("product_id_count_quantity", F.count("quantity").over(w2))
    .withColumn("unit_price_avg_2_prior", F.avg("unit_price").over(w3))
    .withColumn("row_seq", F.row_number().over(w4))
)

enriched = (
    scored
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.price"))
)

scored_order_items = (
    enriched
    .select(
        "*",
        F.ntile(4).over(w5).alias("price_bucket"),
        F.count("product_id_count_quantity").over(w_hist).alias("order_id_count_product_id_count_quantity"),
        F.cume_dist().over(w_recent).alias("quantity_cume_dist"),
    )
)

with_dims = (
    scored_order_items
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.product_name"), F.col("p.category"), F.col("p.cost"))
)

enriched_order_items = (
    with_dims
    .alias("e")
    .join(df_products.alias("b"), F.col("e.product_id") == F.col("b.id"), "inner")
    .select(
        "e.*",
        F.col("b.category").alias("product_category"),
        F.col("b.product_name").alias("product_product_name"),
        F.col("b.price").alias("product_price"),
    )
)

joined = (
    enriched_order_items
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.discount"), F.col("b.status"), F.col("b.amount"))
)

with_dims_order_items = (
    joined
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.status").alias("order_status"))
)

result = (
    with_dims_order_items
    .groupBy("product_id", "order_status")
    .pivot("category", ["Home", "Fashion"])
    .agg(F.max("quantity_cume_dist").alias("value_sum"), F.count(F.lit(1)).alias("cnt"))
)
