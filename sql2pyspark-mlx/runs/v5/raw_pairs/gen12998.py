from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("cost").desc_nulls_first())
w2 = Window.partitionBy("product_id").orderBy(F.col("cost").asc_nulls_last())

cleaned = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        F.when(F.col("unit_price") >= 410, "High").when(F.col("unit_price") >= 190, "Medium").otherwise("Low").alias("unit_price_tier"),
        F.round((F.col("unit_price") * 0.9), 2).alias("unit_price_adjusted"),
    )
)

remaining_keys = (
    cleaned
    .select("id", "product_id")
    .subtract(
        df_order_items
        .filter(F.col("unit_price").between(220, 390))
        .select("id", "product_id")
    )
)

kept_rows = (
    cleaned
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

joined = (
    kept_rows
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.category"), F.col("p.price"), F.col("p.cost"))
)

with_windows = (
    joined
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        "unit_price_tier",
        "price",
        F.dense_rank().over(w).alias("cost_dense_rank"),
        F.cume_dist().over(w2).alias("cost_cume_dist"),
    )
)

enriched = (
    with_windows
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.category") == "Grocery")
    .select("s.*", F.col("p.price").alias("product_price"))
)

result = (
    enriched
    .groupBy("product_id")
    .agg(
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.countDistinct("product_id").alias("distinct_product_ids_2"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.percentile_approx("cost_dense_rank", 0.5).alias("cost_dense_rank_p50"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
