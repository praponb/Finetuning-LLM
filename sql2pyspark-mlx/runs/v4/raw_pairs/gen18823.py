from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.length("supplier_name") < 9)
    .select(
        "id",
        "city",
        "supplier_name",
        "country",
        F.trim("supplier_name").alias("clean_supplier_name"),
    )
)
