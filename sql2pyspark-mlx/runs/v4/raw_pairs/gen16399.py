from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("sqft").between(4470, 9196) & F.col("region").isin(["East", "North"])) | (F.col("sqft") >= 9662))
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(
        F.sum("sqft").alias("total_sqft"),
        F.sum(F.when(F.col("city") == "Tokyo", F.col("sqft")).otherwise(0)).alias("tokyo_sqft"),
    )
    .orderBy("opened_month")
)
