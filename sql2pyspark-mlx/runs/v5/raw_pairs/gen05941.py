from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("sqft") != 11099) & F.col("region").isNotNull())
    .select("id", "store_name", "city", "opened_date")
)
