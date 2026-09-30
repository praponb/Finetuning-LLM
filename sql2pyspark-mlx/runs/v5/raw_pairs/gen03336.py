from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit").isNotNull())
    .groupBy("country")
    .agg(
        F.max("age").alias("max_age"),
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("country") == "TH", 1)).alias("th_count"),
    )
    .orderBy(F.col("row_count").desc())
)
