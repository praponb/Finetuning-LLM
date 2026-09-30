from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") <= 6401)
    .select(
        "id",
        "sqft",
        "opened_date",
        F.substring("store_name", 1, 3).alias("store_name_prefix"),
    )
)
