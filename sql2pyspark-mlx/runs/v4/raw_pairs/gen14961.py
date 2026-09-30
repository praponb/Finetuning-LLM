from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").isNull() & F.col("store_name").like("%Exp%"))
    .select("store_name", "opened_date", "sqft")
)
