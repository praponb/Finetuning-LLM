from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") >= 12368)
    .select("id", "opened_date", "store_name", "city")
)
