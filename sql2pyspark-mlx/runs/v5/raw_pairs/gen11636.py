from pyspark.sql import functions as F

base = (
    df_stores
    .filter((F.col("opened_date") >= F.to_date(F.lit("2023-07-07"))) & F.col("region").isNotNull())
    .withColumn("city_title", F.initcap(F.trim("city")))
    .withColumn("store_name_prefix", F.substring("store_name", 1, 3))
    .withColumn("resolved_region", F.coalesce("region", F.lit("N/A")))
)

result = (
    base
    .select("region")
    .subtract(
        df_stores
        .filter(F.col("opened_date") < "2024-01-01")
        .select("region")
    )
)
