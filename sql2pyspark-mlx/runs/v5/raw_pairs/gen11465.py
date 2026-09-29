from pyspark.sql import functions as F

result = (
    df_stores
    .select("sqft", F.regexp_replace("store_name", " ", "-").alias("store_name_slug"))
)
