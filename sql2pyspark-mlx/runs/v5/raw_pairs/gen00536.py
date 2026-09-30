from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city") == "Bangkok")
    .select("id", "supplier_name", "country")
)
