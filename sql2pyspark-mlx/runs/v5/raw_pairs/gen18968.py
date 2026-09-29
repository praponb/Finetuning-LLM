from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("opened_date") < "2023-12-01") & F.col("store_name").like("Si%"))
    .select("id", "sqft")
    .orderBy(F.col("id").desc())
    .limit(10)
)
