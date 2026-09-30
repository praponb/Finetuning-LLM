from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city") == "Berlin")
    .select(
        "country",
        "rating",
        "city",
        F.concat_ws("-", "supplier_name", "country").alias("supplier_name_country_key"),
        F.concat("country", F.lit(" - "), "city").alias("country_city"),
    )
)
