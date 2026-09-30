from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city") == "Chiang Mai")
    .select(
        "id",
        "city",
        "supplier_name",
        "rating",
        F.trim("supplier_name").alias("clean_supplier_name"),
    )
)
