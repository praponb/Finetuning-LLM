from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isNotNull() & F.col("store_name").isNotNull())
    .select("id", "region", "sqft", "opened_date")
    .orderBy(F.col("sqft").desc())
)
