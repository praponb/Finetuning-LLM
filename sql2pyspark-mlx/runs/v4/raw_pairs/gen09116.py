from pyspark.sql import functions as F

with_dims = (
    df_store_sales
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.price"), F.col("b.cost"))
)

result = (
    with_dims
    .withColumn("cost_tier", F.when(F.col("cost") >= 700, "PREMIUM").otherwise("STANDARD"))
    .cube("product_id", "cost_tier")
    .agg(
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.percentile_approx("units", 0.9).alias("units_p90"),
        F.grouping_id().alias("grouping_id"),
    )
)
