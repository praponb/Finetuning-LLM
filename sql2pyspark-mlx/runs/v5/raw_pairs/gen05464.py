from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isin(["Chiang Mai", "Bangkok", "Singapore"]))
    .agg(
        F.count("*").alias("row_count"),
        F.sum("rating").alias("total_rating"),
        F.sum(F.when(F.col("city") == "Chiang Mai", 1).otherwise(0)).alias("chiang_mai_count"),
    )
    .orderBy(F.col("total_rating").desc(), F.col("row_count").desc())
)
