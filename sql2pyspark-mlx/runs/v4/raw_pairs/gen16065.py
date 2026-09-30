from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy("revenue", "id")
w2 = Window.partitionBy("store_id").orderBy("revenue", "id")
w3 = Window.partitionBy("city").orderBy("units", "id")

with_dims = (
    df_store_sales
    .alias("a")
    .join(df_stores.alias("b"), F.col("a.store_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.city"), F.col("b.sqft"), F.col("b.opened_date"))
)

enriched = (
    with_dims
    .alias("e")
    .join(df_products.alias("b"), F.col("e.product_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.launch_date"))
)

metrics = (
    enriched
    .select(
        "*",
        F.lead("sqft", 2).over(w).alias("next_sqft"),
        F.ntile(3).over(w2).alias("revenue_bucket"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

totals = (
    metrics
    .groupBy(
        F.when(F.col("revenue") >= 3700, "HIGH").otherwise("LOW").alias("revenue_tier"),
    )
    .agg(
        F.size(F.collect_set("city")).alias("num_cities"),
        F.sum(F.when(F.col("city") == "Tokyo", F.col("next_sqft")).otherwise(0)).alias("tokyo_next_sqft"),
        F.countDistinct("city").alias("distinct_cities"),
        F.sum(F.when(F.col("city") == "Singapore", F.col("sqft")).otherwise(0)).alias("singapore_sqft"),
        F.round(F.sum("revenue_bucket") / F.nullif(F.count(F.when(F.col("city") == "New York", 1)), F.lit(0)), 2).alias("revenue_bucket_per_new_york"),
        F.percentile_approx("sqft", [0.25, 0.5, 0.75]).alias("sqft_quartiles"),
    )
)

result = (
    totals
    .select(
        "revenue_tier",
        "tokyo_next_sqft",
        "distinct_cities",
        "revenue_bucket_per_new_york",
    )
    .orderBy(F.col("distinct_cities").desc())
)
