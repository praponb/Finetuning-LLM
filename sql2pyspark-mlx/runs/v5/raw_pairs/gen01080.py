from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isNotNull() & F.col("city").isin(["Tokyo", "New York", "Chiang Mai"]))
    .groupBy("city", "country")
    .agg(
        F.count("*").alias("total_count"),
        F.sum(F.when(F.col("country") == "UK", 1).otherwise(0)).alias("uk_count"),
    )
    .filter(F.col("uk_count") >= 62)
    .drop("uk_count")
    .orderBy("city", "country")
)
