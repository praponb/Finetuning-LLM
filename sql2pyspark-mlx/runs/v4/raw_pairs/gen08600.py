from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["South", "North", "Central"]))
    .groupBy("region")
    .agg(
        F.count("sqft").alias("sqft_count"),
        F.min("opened_date").alias("first_opened_date"),
    )
    .orderBy(F.col("region").desc())
    .limit(10)
)
