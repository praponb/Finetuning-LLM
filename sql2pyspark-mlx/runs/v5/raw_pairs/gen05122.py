from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.upper(F.trim("store_name")) == "AIRPORT KIOSK")
    .select(
        "id",
        "region",
        "store_name",
        "opened_date",
        F.initcap(F.trim("city")).alias("city_title"),
        F.when(F.col("city") == "Chiang Mai", 1).otherwise(0).alias("is_chiang_mai"),
    )
)
