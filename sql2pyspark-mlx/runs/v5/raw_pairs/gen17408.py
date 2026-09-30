from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.quarter("launch_date").alias("launch_quarter"),
        F.when(F.col("price") >= 1080, "high").when(F.col("price") >= 300, "mid").otherwise("low").alias("price_bucket"),
        "category",
    )
    .agg(
        F.countDistinct("cost").alias("unique_brands"),
        F.round(((100 * F.sum(F.when(F.col("brand") == "Acme", 1).otherwise(0))) / F.count("*")), 1).alias("pct_acme"),
        F.sum("price").alias("total_price"),
    )
)
