from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("country") == "SG", 1).otherwise(0))) / F.count("*")), 1).alias("pct_sg"),
        F.sum(F.when(F.col("country") == "US", 1).otherwise(0)).alias("us_count"),
    )
    .filter(F.col("us_count") > 56)
    .select("country", "pct_sg", "us_count")
)
