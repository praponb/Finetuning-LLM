from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isin(["New York", "Chiang Mai"]) & F.col("supplier_name").like("Eur%"))
    .select("id", "supplier_name", "rating", "country")
)
