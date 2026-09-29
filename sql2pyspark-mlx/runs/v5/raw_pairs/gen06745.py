from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("rating").desc_nulls_first())
w2 = Window.orderBy(F.col("cost").desc_nulls_first())
w3 = Window.partitionBy("supplier_id")
w4 = Window.orderBy(F.col("rating_cume_dist").desc())
w5 = Window.orderBy(F.col("margin").asc_nulls_first())

base = (
    df_products
    .filter(F.col("brand").isNotNull())
    .withColumn("margin", (F.col("cost") - F.col("price")))
)

kept = (
    base
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.product_id") == 13), "left_semi")
)

kept_keys = (
    kept
    .select("supplier_id")
    .intersect(
        df_products
        .filter(F.size("tags") > 1)
        .select("supplier_id")
    )
)

kept_rows = (
    kept
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.supplier_id") == F.col("k.supplier_id")), "inner")
    .select("s.*")
)

with_dims = (
    kept_rows
    .alias("src")
    .join(df_suppliers.alias("d"), F.col("src.supplier_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.rating"))
)

with_windows = with_dims.withColumn("rating_cume_dist", F.cume_dist().over(w))

scored = (
    with_windows
    .withColumn("cost_percent_rank", F.percent_rank().over(w2))
    .withColumn("corr_cost_price", F.corr("cost", "price").over(w3))
    .withColumn("rating_cume_dist_cume_dist", F.cume_dist().over(w4))
    .withColumn("margin_percent_rank", F.percent_rank().over(w5))
)

cleaned = (
    scored
    .filter(F.col("launch_date") >= F.lit("2023-06-28").cast("date"))
    .select("*", (F.col("rating") * F.col("price")).alias("rating_times_price"))
)

result = (
    cleaned
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "stock_qty",
        "launch_date",
        "tags",
        "rating_cume_dist",
        F.when(F.col("category").rlike("^(Electronics|Books)$"), "GROUP_A").when(F.col("category").rlike("^(Home|Sports)$"), "CORE").otherwise("PRIORITY").alias("category_zone"),
    )
    .orderBy(F.col("stock_qty").desc(), "category")
)
