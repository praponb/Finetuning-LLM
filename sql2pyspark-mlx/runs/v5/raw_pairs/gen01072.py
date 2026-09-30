from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("is_active")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("country") == "SG", 1).otherwise(0))) / F.count("*")), 1).alias("pct_sg"),
    )
    .filter(F.col("pct_sg") > 60)
    .select("is_active", "pct_sg")
    .orderBy(F.col("pct_sg").desc())
)
