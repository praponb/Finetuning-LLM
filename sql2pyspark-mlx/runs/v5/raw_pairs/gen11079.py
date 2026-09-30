from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("city").isin(["London", "Tokyo"]) & F.col("supplier_name").like("E%")) | F.upper("supplier_name").like("%URO%"))
)
