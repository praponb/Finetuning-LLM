from pyspark.sql import functions as F

result = (
    df_suppliers
    .select(
        "id",
        "city",
        "supplier_name",
        "rating",
        F.substring("country", 1, 3).alias("country_prefix"),
        F.concat_ws("/", "city", "country").alias("city_country_key"),
    )
    .orderBy("city", "id")
    .limit(5)
)
