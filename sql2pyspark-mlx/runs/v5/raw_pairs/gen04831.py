from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.1, "PREMIUM").otherwise("STANDARD").alias("rating_tier"),
    )
    .agg(
        F.count(F.when(F.col("country") == "SG", 1)).alias("sg_rows"),
        F.sum(F.when(F.col("country") == "SG", F.col("rating")).otherwise(0)).alias("sg_rating"),
    )
)
