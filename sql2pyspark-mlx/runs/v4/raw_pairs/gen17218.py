from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isNotNull() & F.col("city").isin(["Chiang Mai", "Tokyo"]))
    .select("id", "rating", "supplier_name", "country")
)
