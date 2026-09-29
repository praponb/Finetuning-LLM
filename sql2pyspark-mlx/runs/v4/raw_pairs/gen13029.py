from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") <= 4.1)
    .select("id", "supplier_name")
    .subtract(
        df_suppliers
        .filter(F.col("supplier_name").isNotNull())
        .select("id", "supplier_name")
    )
)
