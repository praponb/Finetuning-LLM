from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") <= 2.5)
    .select("id", "country", "city", "supplier_name")
)
