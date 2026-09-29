from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("category").orderBy(F.col("stock_qty").desc(), "id")
w3 = Window.partitionBy("category").orderBy(F.col("tags_count").desc_nulls_last())

base = (
    df_products
    .filter((F.col("launch_date") >= F.lit("2023-09-08").cast("date")) & F.col("brand").isNotNull())
    .withColumn("tags_count", F.size("tags"))
)

latest_match = (
    base
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.product_name",
        "s.category",
        "s.brand",
        "s.stock_qty",
        "s.tags",
        "s.tags_count",
        F.col("c.quantity").alias("inventory_quantity"),
        F.col("c.last_restock"),
    )
)

deduped = (
    latest_match
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
    .select("id", "category", "tags_count", "inventory_quantity", "last_restock")
)

kept = (
    deduped
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.last_restock") <= F.col("s.last_restock")), "left_semi")
)

deduped_products = (
    kept
    .withColumn("rn", F.rank().over(w3))
    .filter(F.col("rn") == 1)
    .select("id", "category", "tags_count", "inventory_quantity")
)

filtered = (
    deduped_products
    .filter(F.col("category").isin(["Electronics", "Home"]))
    .select("*", F.initcap(F.trim("category")).alias("category_title"))
)

remaining_keys = (
    filtered
    .select("category")
    .exceptAll(
        df_products
        .filter(F.col("category") == "Electronics")
        .select("category")
    )
)

verified = (
    filtered
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.category") == F.col("k.category")), "inner")
    .select("s.*")
)

result = (
    verified
    .select("id", "tags_count", "inventory_quantity", "category_title")
    .orderBy(F.col("id").desc_nulls_first(), F.col("tags_count").desc())
)
