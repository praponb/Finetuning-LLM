from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("region") == "Central") & (F.col("region") == "North"))
    .select(
        "opened_date",
        F.substring("store_name", 1, 3).alias("store_name_prefix"),
        F.regexp_replace("store_name", " ", "").alias("store_name_slug"),
    )
)
