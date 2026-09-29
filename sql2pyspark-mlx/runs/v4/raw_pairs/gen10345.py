from pyspark.sql import functions as F

with_dims = (
    df_inventory
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.cost"), F.col("p.category"), F.col("p.product_name"))
)

result = (
    with_dims
    .withColumn("year_start", F.date_trunc("YEAR", "last_restock"))
    .withColumn("quantity_tier", F.when(F.col("quantity") >= 210, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["year_start", "quantity_tier"], ["year_start"]],
        "year_start",
        "quantity_tier",
    )
    .agg(
        F.countDistinct("category").alias("distinct_categories"),
        F.countDistinct("category").alias("distinct_categories_2"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.count(F.when(F.col("category") == "Home", 1)).alias("home_count"),
        F.grouping_id().alias("grouping_id"),
    )
)
