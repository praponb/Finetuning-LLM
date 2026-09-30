from pyspark.sql import functions as F

enriched = (
    df_order_items
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "inner")
    .select(
        "src.id",
        "src.order_id",
        "src.product_id",
        "src.unit_price",
        F.col("p.category"),
        F.col("p.launch_date"),
        F.col("p.cost"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("cost") >= 660, "LARGE").otherwise("SMALL").alias("cost_tier"),
    )
    .agg(
        F.size(F.collect_set("category")).alias("num_categories"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.size(F.collect_set("category")).alias("num_categories_2"),
    )
    .filter(F.col("num_categories_2") > 1)
    .orderBy(F.col("num_categories").desc_nulls_first())
)
