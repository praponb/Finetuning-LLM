from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city") == "Berlin")
    .select("id", "supplier_name", "city")
)
