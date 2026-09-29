from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .agg(
        F.max("launch_date").alias("last_launch_date"),
        F.round(((100 * F.sum(F.when(F.col("category") == "Grocery", 1).otherwise(0))) / F.count("*")), 1).alias("pct_grocery"),
    )
)
