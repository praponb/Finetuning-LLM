from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating").between(2.6, 2.8) & F.col("country").isin(["DE", "JP"]))
    .select(
        "id",
        "city",
        F.concat("supplier_name", F.lit("/"), "city").alias("supplier_name_city"),
        F.length("city").alias("city_length"),
    )
    .orderBy(F.col("city_length").desc(), "supplier_name_city")
)
