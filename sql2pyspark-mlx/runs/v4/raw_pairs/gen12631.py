from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id")
w2 = Window.partitionBy("order_id")
w3 = Window.orderBy("quantity", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.orderBy(F.col("quantity").desc())
w5 = Window.partitionBy("product_id")
w6 = Window.partitionBy("product_id").orderBy("order_id_max_unit_price", "id").rowsBetween(-2, -1)
w7 = Window.partitionBy("order_id").orderBy("quantity_avg_3", "id").rowsBetween(-3, Window.currentRow)
w8 = Window.partitionBy("category").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_dims = (
    df_order_items
    .alias("s")
    .join(df_products.alias("d"), F.col("s.product_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.category"))
)

metrics = (
    with_dims
    .withColumn("unit_price_share", F.round((F.col("unit_price") / F.sum("unit_price").over(w)), 4))
    .withColumn("order_id_max_unit_price", F.max("unit_price").over(w2))
    .withColumn("quantity_avg_3", F.avg("quantity").over(w3))
    .withColumn("quantity_percent_rank", F.percent_rank().over(w4))
)

with_dims_order_items = (
    metrics
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "inner")
    .select("src.*", F.col("p.stock_qty"), F.col("p.launch_date"), F.col("p.price"))
)

joined = (
    with_dims_order_items
    .alias("s")
    .join(df_products.alias("d"), F.col("s.product_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.launch_date").alias("product_launch_date"))
)

scored = (
    joined
    .withColumn("unit_price_share_2", F.round((F.col("unit_price") / F.sum("unit_price").over(w5)), 4))
    .withColumn("unit_price_share_avg_2_prior", F.avg("unit_price_share").over(w6))
    .withColumn("price_share", F.round((F.col("price") / F.sum("price").over(w2)), 4))
    .withColumn("quantity_percent_rank_avg_3", F.avg("quantity_percent_rank").over(w7))
    .withColumn("running_quantity_avg_3", F.sum("quantity_avg_3").over(w8))
)

enriched = (
    scored
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.launch_date").alias("product_launch_date_2"))
)

result = (
    enriched
    .groupBy(
        "order_id",
        F.date_trunc("QUARTER", "product_launch_date_2").alias("quarter_start"),
    )
    .agg(
        F.count_if(F.col("category") == "Books").alias("books_events"),
        F.max(F.when(F.col("category") == "Grocery", F.col("unit_price_share_avg_2_prior"))).alias("grocery_max_unit_price_share_avg_2_prior"),
        F.count_if(F.col("category") == "Sports").alias("sports_events"),
        F.count(F.when(F.col("category") == "Books", 1)).alias("books_count"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
    )
)
