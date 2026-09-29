from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("name").like("%Chen%"))
    .groupBy("country")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "Bangkok", 1).otherwise(0))) / F.count("*")), 1).alias("pct_bangkok"),
    )
)
