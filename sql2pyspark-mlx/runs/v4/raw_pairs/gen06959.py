from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") <= 2.9)
    .orderBy(F.col("rating").desc(), "supplier_name")
)
