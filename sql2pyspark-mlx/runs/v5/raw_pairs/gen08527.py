from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") > 10551)
    .select(
        "id",
        "region",
        "store_name",
        F.ceil("sqft").alias("sqft_whole"),
        F.ceil("sqft").alias("sqft_whole_2"),
    )
)
