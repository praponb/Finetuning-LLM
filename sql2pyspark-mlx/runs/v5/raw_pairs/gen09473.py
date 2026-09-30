from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "inner")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .groupBy(
        F.when(F.col("p.stock_qty") >= 382, "high").when(F.col("p.stock_qty") >= 166, "mid").otherwise("low").alias("stock_qty_bucket"),
        F.when(F.col("p.brand").isin(["Globex", "Umbrella"]), "core").otherwise("other").alias("brand_group"),
    )
    .agg(
        F.count("*").alias("num_inventory"),
        F.avg(F.col("i.quantity")).alias("avg_quantity"),
    )
    .orderBy(
        "avg_quantity",
        F.col("num_inventory").desc(),
        "stock_qty_bucket",
        "brand_group",
    )
    .limit(3)
)
