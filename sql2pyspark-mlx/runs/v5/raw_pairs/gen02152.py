from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") == "UK")
    .select("id", "country", "supplier_name")
    .subtract(
        df_suppliers
        .filter(F.col("supplier_name").like("S%"))
        .select("id", "country", "supplier_name")
    )
    .orderBy(F.col("id").desc())
)
