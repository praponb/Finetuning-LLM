from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w_running = Window.partitionBy("category").orderBy("cost", "id")

valid = (
    df_products
    .filter(F.col("launch_date") > F.lit("2023-02-26").cast("date"))
    .select(
        "id",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        F.length("brand").alias("brand_length"),
    )
)

with_last = (
    valid
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.category",
        "s.price",
        "s.cost",
        "s.brand_length",
        F.col("c.unit_price").alias("order_item_unit_price"),
    )
)

with_last_products = (
    with_last
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.unit_price").alias("order_item_unit_price_2"))
)

with_windows = (
    with_last_products
    .withColumn("next_order_item_unit_price", F.lead("order_item_unit_price").over(w_running))
    .withColumn("prev_cost", F.lag("cost").over(w_running))
    .withColumn("next_cost", F.lead("cost").over(w_running))
    .withColumn("row_seq", F.row_number().over(w_running))
)

result = (
    with_windows
    .withColumn("brand_length_tier", F.when(F.col("brand_length") >= 66, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["brand_length_tier", "category"], ["brand_length_tier"], []],
        "brand_length_tier",
        "category",
    )
    .agg(
        F.count(F.when(F.col("category") == "Books", 1)).alias("books_count"),
        F.size(F.collect_set("category")).alias("num_categories"),
        F.round(F.sum("next_cost") / F.nullif(F.count(F.when(F.col("category") == "Books", 1)), F.lit(0)), 2).alias("next_cost_per_books"),
        F.size(F.collect_set("category")).alias("num_categories_2"),
    )
    .orderBy(F.col("num_categories").desc_nulls_last())
)
