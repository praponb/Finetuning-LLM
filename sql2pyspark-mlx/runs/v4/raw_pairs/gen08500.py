from pyspark.sql import functions as F

filtered = (
    df_stores
    .filter((F.col("opened_date") >= F.lit("2023-09-08").cast("date")) & F.col("region").isNotNull())
    .withColumn("region_prefix", F.substring("region", 1, 4))
    .withColumn("clean_store_name", F.trim("store_name"))
    .withColumn("region_filled", F.coalesce("region", F.lit("OTHER")))
)

result = (
    filtered
    .select("region")
    .subtract(
        df_stores
        .filter(F.col("city") != "New York")
        .select("region")
    )
)
