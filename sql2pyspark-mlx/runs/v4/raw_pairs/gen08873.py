from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("G%") | F.col("supplier_name").like("%Global%"))
    .select(
        "id",
        "supplier_name",
        "city",
        F.concat("city", F.lit(" "), "supplier_name").alias("city_supplier_name"),
    )
)
