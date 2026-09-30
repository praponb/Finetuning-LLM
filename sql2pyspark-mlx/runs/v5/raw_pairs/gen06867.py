from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id")
w2 = Window.partitionBy("brand")
w3 = Window.partitionBy("supplier_id", "brand")
w4 = Window.partitionBy("category", "supplier_id")
w5 = Window.orderBy(F.col("cost").desc())

remaining_keys = (
    df_products
    .select("category")
    .subtract(
        df_products
        .filter(F.month("launch_date").isin([1, 6, 12]))
        .select("category")
    )
)

verified = (
    df_products
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.category") == F.col("k.category")), "inner")
    .select("s.*")
)

kept = (
    verified
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.unit_price") < 130), "left_semi")
)

metrics = (
    kept
    .filter(F.col("brand").isin(["Stark", "Globex", "Initech"]))
    .select(
        "*",
        F.round((F.col("price") / F.sum("price").over(w)), 4).alias("price_share"),
        F.count("cost").over(w2).alias("brand_count_cost"),
        F.max("cost").over(w3).alias("supplier_id_max_cost"),
        F.count("cost").over(w4).alias("category_count_cost"),
        F.rank().over(w5).alias("cost_rank"),
    )
)

enriched = (
    metrics
    .alias("src")
    .join(df_suppliers.alias("d"), F.col("src.supplier_id").eqNullSafe(F.col("d.id")), "left")
    .select("src.*", F.col("d.supplier_name"), F.col("d.country"), F.col("d.rating"))
)

result = (
    enriched
    .select(
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
    )
    .orderBy(F.col("cost").desc_nulls_last())
)
