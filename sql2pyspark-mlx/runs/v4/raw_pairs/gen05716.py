from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.length("store_name") >= 9)
    .select(
        "id",
        "store_name",
        F.concat_ws("-", "region", "store_name").alias("region_store_name_key"),
    )
)
