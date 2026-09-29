from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.year("launch_date") == 2025)
    .groupBy("category", "supplier_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("category") == "Sports", 1).otherwise(0))) / F.count("*")), 1).alias("pct_sports"),
        F.round(F.avg("price"), 2).alias("avg_price"),
    )
    .filter(F.col("avg_price") < 30)
    .select("category", "supplier_id", "pct_sports", "avg_price")
)
