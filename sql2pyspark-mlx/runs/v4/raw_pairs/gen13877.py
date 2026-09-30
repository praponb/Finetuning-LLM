from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category", "supplier_id").orderBy("price", "id")
w2 = Window.partitionBy("category").orderBy("launch_date", "id")
w3 = Window.partitionBy("supplier_id")
w4 = Window.partitionBy("category", "supplier_id").orderBy("launch_date", "id")
w5 = Window.partitionBy("supplier_id").orderBy(F.col("launch_date").desc(), "id")
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())
cost_by_supplier_id = df_products.groupBy("supplier_id").agg(F.min("cost").alias("min_product_cost"))

filtered = (
    df_products
    .filter(F.col("supplier_id").isNotNull() & (F.col("stock_qty") <= 314))
    .select(
        "id",
        "product_name",
        "category",
        "price",
        "launch_date",
        "supplier_id",
        F.when(F.col("price") >= 1220, "High").when(F.col("price") >= 230, "Medium").otherwise("Low").alias("price_band"),
        F.coalesce("brand", F.lit("N/A")).alias("brand_clean"),
    )
)

kept = (
    filtered
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.last_restock") <= F.col("s.launch_date")), "left_semi")
)

metrics = (
    kept
    .select(
        "*",
        F.lead("price", 1).over(w).alias("next_price"),
        F.lead("price", 1).over(w2).alias("next_price_2"),
        F.count("price").over(w3).alias("supplier_id_count_price"),
    )
)

ranked = (
    metrics
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

deduped = (
    ranked
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

latest_match = (
    deduped
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.revenue").alias("store_sale_revenue"), F.col("c.units"))
)

result = (
    latest_match
    .join(cost_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "product_name",
        "category",
        "launch_date",
        "supplier_id",
        "price_band",
        "brand_clean",
        "next_price",
        "next_price_2",
        F.col("min_product_cost"),
    )
    .orderBy("category")
)
