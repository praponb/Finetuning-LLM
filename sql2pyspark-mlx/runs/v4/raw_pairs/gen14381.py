from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").isNotNull())
    .select(
        "id",
        "opened_date",
        "sqft",
        F.substring("store_name", 1, 4).alias("store_name_prefix"),
    )
    .orderBy(F.col("sqft").desc())
)
