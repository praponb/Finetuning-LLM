from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") > 2.0)
    .select("supplier_name", "city")
)
