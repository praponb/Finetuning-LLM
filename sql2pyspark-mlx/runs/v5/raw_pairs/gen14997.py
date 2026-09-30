from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.upper("store_name").like("%OWN%"))
    .select("city", "store_name", "sqft")
)
