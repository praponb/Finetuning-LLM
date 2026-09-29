from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.when(F.col("price") >= 1030, "PREMIUM").otherwise("STANDARD").alias("price_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("category") == "Grocery", 1)).alias("grocery_count"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
    )
    .orderBy(F.col("num_rows").desc())
)
