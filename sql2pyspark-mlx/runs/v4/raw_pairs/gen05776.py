from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country") == "TH")
    .select(
        "id",
        "city",
        "segment",
        F.when(F.col("age") >= 70, "High").when(F.col("age") >= 30, "Medium").otherwise("Low").alias("age_level"),
        F.concat_ws("/", "city", "country").alias("city_country_key"),
    )
)
