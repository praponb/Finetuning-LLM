from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date").between("2025-01-01", "2025-10-07") & (F.col("sqft") == 15406))
    .groupBy("region")
    .agg(
        F.sum("sqft").alias("total_sqft"),
        F.max("sqft").alias("max_sqft"),
    )
)
