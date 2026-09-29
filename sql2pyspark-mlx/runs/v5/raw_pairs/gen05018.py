from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "eco") & (F.col("cost") > 360))
    .groupBy("brand")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("category") == "Sports", 1).otherwise(0))) / F.count("*")), 1).alias("pct_sports"),
        F.count("cost").alias("cost_count"),
    )
    .orderBy(F.col("pct_sports").desc(), "brand")
)
