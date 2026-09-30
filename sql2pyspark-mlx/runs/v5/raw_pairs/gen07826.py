from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("cost").desc())
w_running = Window.partitionBy("store_id").orderBy("least_units_revenue", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("brand")

valid = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-05-26").cast("date"))
    .withColumn("revenue_per_unit", F.round((F.col("units") / F.col("revenue")), 2))
    .withColumn("least_units_revenue", F.least("units", "revenue"))
)

with_dims = (
    valid
    .alias("s")
    .join(df_products.alias("b"), F.col("s.product_id") == F.col("b.id"), "inner")
    .select("s.*", F.col("b.launch_date"), F.col("b.cost"), F.col("b.brand"))
)

deduped = (
    with_dims
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

metrics = (
    deduped
    .select(
        "id",
        "product_id",
        "units",
        "revenue",
        "revenue_per_unit",
        "least_units_revenue",
        "launch_date",
        "cost",
        "brand",
        F.sum("revenue").over(w_running).alias("last3_sum_revenue"),
        F.count("revenue").over(w_running).alias("last3_count_revenue"),
        F.min("revenue").over(w_running).alias("last3_min_revenue"),
        F.sum("least_units_revenue").over(w3).alias("brand_sum_least_units_revenue"),
    )
)

enriched = (
    metrics
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "inner")
    .select(
        "src.id",
        "src.product_id",
        "src.units",
        "src.revenue",
        "src.least_units_revenue",
        "src.cost",
        "src.brand",
        "src.last3_count_revenue",
        "src.brand_sum_least_units_revenue",
        F.col("p.brand").alias("product_brand"),
        F.col("p.stock_qty"),
        F.col("p.cost").alias("product_cost"),
    )
)

result = (
    enriched
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "inner")
    .groupBy(
        F.col("p.brand").alias("product_brand"),
        F.col("e.brand").alias("store_sale_brand"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("e.product_cost")).alias("max_product_cost"),
        F.min(F.col("e.least_units_revenue")).alias("min_least_units_revenue"),
    )
    .orderBy("product_brand")
)
