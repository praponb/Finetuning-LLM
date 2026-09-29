from pyspark.sql import functions as F

result = (
    df_stores
    .select(
        "id",
        "store_name",
        "region",
        "city",
        F.initcap(F.trim("store_name")).alias("store_name_title"),
    )
)
