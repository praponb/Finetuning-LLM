from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isNull() | (F.col("rating") > 2.9))
    .select(
        "id",
        "city",
        "supplier_name",
        "rating",
        F.concat("country", F.lit(" "), "supplier_name").alias("country_supplier_name"),
    )
    .orderBy(F.col("rating").desc(), "country_supplier_name")
)
