from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").isNotNull() | ~F.col("country").isin(["JP", "UK"]))
    .orderBy(F.col("supplier_name").desc())
)
