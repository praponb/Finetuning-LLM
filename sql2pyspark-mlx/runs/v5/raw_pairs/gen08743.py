from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city") == "Tokyo")
    .select("sqft", F.quarter("opened_date").alias("opened_date_quarter"))
)
