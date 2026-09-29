from pyspark.sql import functions as F

result = (
    df_stores
    .filter(((F.col("city") == "Chiang Mai") & (F.col("opened_date") >= "2025-08-17")) | (F.col("opened_date") <= "2024-03-01"))
    .select("id", "opened_date", "region", F.round("sqft", 2).alias("sqft_rounded"))
    .orderBy("id")
    .limit(5)
)
