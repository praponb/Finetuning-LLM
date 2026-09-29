from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("region") == "Central") & (F.year("opened_date") >= 2023))
    .select("id", "opened_date", F.round("sqft", 0).alias("sqft_rounded"))
)
