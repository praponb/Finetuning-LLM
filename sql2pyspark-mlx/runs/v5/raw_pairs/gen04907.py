from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("city") == "Berlin") & ((F.col("opened_date") <= "2025-01-01") | F.col("region").isin(["South", "North", "West"])))
    .agg(F.sum("sqft").alias("total_sqft"))
    .orderBy(F.col("total_sqft").desc())
)
