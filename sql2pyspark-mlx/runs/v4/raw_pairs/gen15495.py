from pyspark.sql import functions as F

result = (
    df_stores
    .select(
        "id",
        "city",
        "opened_date",
        F.when(F.col("sqft") >= 16085, "High").when(F.col("sqft") >= 2602, "Medium").otherwise("Low").alias("sqft_band"),
        F.substring("city", 1, 3).alias("city_prefix"),
    )
)
