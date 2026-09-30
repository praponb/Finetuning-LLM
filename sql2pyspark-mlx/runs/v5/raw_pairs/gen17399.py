from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("sqft") == 13535) & F.col("store_name").like("Ri%"))
    .orderBy("city")
)
