from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id", "category").orderBy("rating")
w2 = Window.partitionBy("category").orderBy(F.col("rating").desc_nulls_first())
w3 = Window.partitionBy("category").orderBy(F.col("cnt").desc())

base = (
    df_products
    .filter(F.col("price") < 1150)
    .select(
        "*",
        F.coalesce("stock_qty", F.lit(0)).alias("stock_qty_filled"),
        F.coalesce("brand", F.lit("N/A")).alias("resolved_brand"),
    )
)

with_dims = (
    base
    .alias("e")
    .join(df_suppliers.alias("p"), F.col("e.supplier_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.rating"))
)

filtered = (
    with_dims
    .filter(F.col("launch_date") > F.lit("2023-06-18").cast("date"))
    .select(
        "id",
        "product_name",
        "category",
        "price",
        "stock_qty",
        "tags",
        "supplier_id",
        "stock_qty_filled",
        "resolved_brand",
        "rating",
        F.lower("product_name").alias("product_name_lower"),
    )
)

not_flagged = (
    filtered
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.store_id").isin([1, 4])), "left_anti")
)

metrics = (
    not_flagged
    .withColumn("rating_dense_rank", F.dense_rank().over(w))
    .withColumn("rating_percent_rank", F.percent_rank().over(w2))
)

eligible = (
    metrics
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.quantity").between(4, 8)), "left_semi")
)

result = (
    eligible
    .groupBy("category", "supplier_id")
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("category").alias("distinct_categories"),
        F.count_if(F.col("category") == "Home").alias("home_events"),
        F.size(F.collect_set("category")).alias("num_categories"),
    )
    .withColumn("cnt_percent_rank", F.percent_rank().over(w3))
    .orderBy(F.col("num_categories").desc())
)
