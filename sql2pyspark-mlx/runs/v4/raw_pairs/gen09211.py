from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") >= 3.8)
    .select("id", "country", "rating", "supplier_name")
)
