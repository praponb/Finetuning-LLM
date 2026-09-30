from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.upper("supplier_name").like("%IAM%"))
    .select(
        "rating",
        "country",
        "city",
        F.concat_ws("-", "country", "supplier_name").alias("country_supplier_name_key"),
    )
)
