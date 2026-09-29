from pyspark.sql import functions as F

result = (
    df_stores
    .select(
        "id",
        "opened_date",
        "region",
        "city",
        F.substring("store_name", 1, 2).alias("store_name_prefix"),
    )
    .orderBy("region", "id")
    .limit(20)
)
