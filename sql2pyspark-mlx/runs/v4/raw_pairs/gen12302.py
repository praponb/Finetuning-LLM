from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "Chiang Mai", 1).otherwise(0))) / F.count("*")), 1).alias("pct_chiang_mai"),
    )
    .filter(F.col("pct_chiang_mai") >= 64)
    .select("city", "pct_chiang_mai")
    .orderBy(F.col("pct_chiang_mai").desc())
)
