from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("region") != "East") & (F.col("sqft") != 9853))
    .select("id", "store_name", "opened_date", "city")
)
