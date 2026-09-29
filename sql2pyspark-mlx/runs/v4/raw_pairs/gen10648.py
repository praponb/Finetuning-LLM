from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("city").isin(["Singapore", "Chiang Mai"]))
    .groupBy("is_active")
    .agg(
        F.countDistinct("credit_limit").alias("unique_citys"),
        F.count("credit_limit").alias("credit_limit_count"),
    )
    .filter(F.col("unique_citys") > 1)
    .orderBy(F.col("credit_limit_count").desc())
)
