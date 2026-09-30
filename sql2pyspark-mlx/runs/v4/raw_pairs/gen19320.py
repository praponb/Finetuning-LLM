from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") >= "2023-10-01")
    .groupBy("city")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("region") == "Central", 1).otherwise(0))) / F.count("*")), 1).alias("pct_central"),
    )
    .filter(F.col("pct_central") < 20)
    .select("city", "pct_central")
    .orderBy("city")
)
