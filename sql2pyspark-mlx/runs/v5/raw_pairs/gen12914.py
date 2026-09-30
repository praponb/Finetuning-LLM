from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("city") == "New York") & (F.col("city") == "Singapore"))
    .select(
        "rating",
        F.concat_ws("/", "country", "city").alias("country_city_key"),
        F.concat("supplier_name", F.lit("/"), "city").alias("supplier_name_city"),
    )
)
