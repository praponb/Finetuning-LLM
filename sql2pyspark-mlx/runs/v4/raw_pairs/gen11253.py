from pyspark.sql import functions as F

result = (
    df_suppliers
    .select(
        "id",
        "rating",
        "city",
        "country",
        F.concat_ws(",", "country", "city").alias("country_city_key"),
    )
    .orderBy(F.col("rating").desc())
)
