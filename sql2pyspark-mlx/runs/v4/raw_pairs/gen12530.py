from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("city") != "London") & (F.col("country") == "DE"))
    .select("country", "supplier_name", "city")
)
