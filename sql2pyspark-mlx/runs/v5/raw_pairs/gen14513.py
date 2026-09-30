from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("city") != "Singapore") | (F.col("sqft") <= 9596))
    .select("id", "opened_date", "store_name", "region")
)
