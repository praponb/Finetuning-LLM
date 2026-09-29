from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.when(F.col("brand").isin(["Umbrella", "Stark"]), "group_a").otherwise("other").alias("brand_group"),
        "category",
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("brand") == "Stark", 1).otherwise(0))) / F.count("*")), 1).alias("pct_stark"),
    )
    .filter(F.col("pct_stark") > 31)
)
