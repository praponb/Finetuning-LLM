from pyspark.sql import functions as F

valid = (
    df_stores
    .filter((F.col("sqft") < 8853) & (F.col("opened_date") >= F.lit("2023-07-06").cast("date")))
    .select(
        "id",
        "store_name",
        "city",
        "opened_date",
        F.lower("city").alias("city_lower"),
        F.when(F.col("region") == "Central", 1).otherwise(0).alias("is_central"),
    )
)

result = (
    valid
    .select("id", "city")
    .intersectAll(
        df_stores
        .filter(F.col("opened_date") < "2025-10-20")
        .select("id", "city")
    )
)
