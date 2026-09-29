from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") <= 6046)
    .groupBy(
        F.when(F.col("sqft") >= 17982, "high").when(F.col("sqft") >= 7635, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(F.count("*").alias("num_stores"))
    .filter(F.col("num_stores") >= 4)
    .select("sqft_bucket", "num_stores")
    .orderBy("sqft_bucket")
)
