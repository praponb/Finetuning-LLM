from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .agg(
        F.count(F.when(F.col("country") == "TH", 1)).alias("th_count"),
        F.avg("rating").alias("avg_rating"),
    )
    .filter(F.col("avg_rating") >= 2.9)
    .orderBy(F.col("th_count").desc())
)
