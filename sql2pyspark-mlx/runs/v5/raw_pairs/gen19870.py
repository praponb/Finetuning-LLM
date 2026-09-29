from pyspark.sql import functions as F

cleaned = (
    df_stores
    .filter((F.col("opened_date") > F.lit("2023-06-20").cast("date")) & F.col("region").isNotNull())
    .withColumn("store_name_city", F.concat("store_name", F.lit(" "), "city"))
    .withColumn("sqft_whole", F.floor("sqft"))
    .withColumn("region_clean", F.coalesce("region", F.lit("N/A")))
)

result = (
    cleaned
    .select("city", "id")
    .subtract(
        df_stores
        .filter(F.col("store_name").like("%Mal%"))
        .select("city", "id")
    )
)
