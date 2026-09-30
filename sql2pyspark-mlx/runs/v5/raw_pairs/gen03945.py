from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["SG", "UK"]) & (F.upper("supplier_name").like("%ACI%") | (F.col("city") != "London")))
    .select(
        "id",
        "country",
        F.concat_ws("-", "supplier_name", "city").alias("supplier_name_city_key"),
    )
)
