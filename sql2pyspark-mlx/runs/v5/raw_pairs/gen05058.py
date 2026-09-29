from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .agg(
        F.sum("price").alias("total_price"),
        F.round(((100 * F.sum(F.when(F.col("brand") == "Umbrella", 1).otherwise(0))) / F.count("*")), 1).alias("pct_umbrella"),
    )
    .filter(F.col("pct_umbrella") > 76)
)
