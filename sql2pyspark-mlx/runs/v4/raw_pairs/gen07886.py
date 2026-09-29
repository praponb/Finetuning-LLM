from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.5, "HIGH").otherwise("LOW").alias("rating_tier"),
        "country",
    )
    .agg(
        F.count_if(F.col("country") == "UK").alias("uk_events"),
        F.count(F.when(F.col("country") == "JP", 1)).alias("jp_rows"),
        F.count_if(F.col("city") == "Tokyo").alias("tokyo_events"),
        F.avg(F.when(F.col("country") == "SG", F.col("rating"))).alias("sg_avg_rating"),
        F.count(F.when(F.col("country") == "JP", 1)).alias("jp_rows_2"),
    )
    .filter(F.col("sg_avg_rating") < 0)
)
