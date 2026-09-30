from pyspark.sql import functions as F

filtered = (
    df_stores
    .filter((F.col("opened_date") >= F.lit("2023-07-25").cast("date")) & F.col("region").isNotNull())
    .withColumn("city_upper", F.upper("city"))
    .withColumn("region_city_key", F.concat_ws("/", "region", "city"))
)

result = (
    filtered
    .select("id", "city")
    .subtract(
        df_stores
        .filter(F.col("sqft") == 5289)
        .select("id", "city")
    )
)
