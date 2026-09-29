from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .agg(
        F.sum(F.when(F.col("country") == "SG", F.col("rating")).otherwise(0)).alias("sg_rating"),
        F.sum("rating").alias("total_rating"),
    )
)
