from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") >= 2.6) & (F.col("rating") > 3.9))
    .select(
        "id",
        "supplier_name",
        "rating",
        "city",
        F.trim("supplier_name").alias("clean_supplier_name"),
    )
)
