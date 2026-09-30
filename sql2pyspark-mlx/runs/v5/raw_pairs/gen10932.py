from pyspark.sql import functions as F

result = (
    df_suppliers
    .select(
        "id",
        "country",
        F.upper("country").alias("country_upper"),
        F.concat_ws(",", "supplier_name", "country").alias("supplier_name_country_key"),
    )
)
