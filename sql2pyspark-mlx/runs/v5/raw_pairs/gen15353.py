from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").isNull())
    .select(
        "id",
        "country",
        "supplier_name",
        F.concat("country", F.lit(" "), "supplier_name").alias("country_supplier_name"),
        F.lower("supplier_name").alias("supplier_name_lower"),
    )
    .orderBy("country", F.col("supplier_name_lower").desc())
)
