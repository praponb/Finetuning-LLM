from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id")
w2 = Window.partitionBy("product_id").orderBy(F.col("quantity").desc())
w3 = Window.partitionBy("store_id").orderBy("price")
w4 = Window.partitionBy("store_id")

cleaned = (
    df_inventory
    .filter(F.col("store_id") == 5)
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.date_format("last_restock", "yyyy-MM").alias("last_restock_label"),
    )
)

kept_keys = (
    cleaned
    .select("product_id", "id")
    .subtract(
        df_inventory
        .filter(F.col("product_id").isin([2, 7]))
        .select("product_id", "id")
    )
)

kept_rows = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    kept_rows
    .filter(F.col("last_restock") < "2025-03-01")
    .select(
        "*",
        F.avg("quantity").over(w).alias("store_id_avg_quantity"),
        F.cume_dist().over(w2).alias("quantity_cume_dist"),
    )
)

joined = (
    metrics
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .select("src.id", "src.store_id", F.col("p.price"))
)

metrics_inventory = (
    joined
    .select(
        "*",
        F.rank().over(w3).alias("price_rank"),
        F.round((F.col("price") / F.sum("price").over(w4)), 4).alias("price_share"),
    )
)

result = (
    metrics_inventory
    .alias("s")
    .join(df_stores.alias("p"), F.col("s.store_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.region").alias("region"))
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("s.price_share")).alias("min_price_share"),
    )
)
