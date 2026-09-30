from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["JP", "UK", "DE"]) & F.col("supplier_name").like("E%"))
    .select(
        "id",
        "supplier_name",
        F.trim("city").alias("clean_city"),
        F.lower("supplier_name").alias("supplier_name_lower"),
    )
)
