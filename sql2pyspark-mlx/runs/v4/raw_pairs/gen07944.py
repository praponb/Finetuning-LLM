from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~F.col("supplier_name").like("E%"))
    .select("supplier_name", "city", "country")
    .orderBy("country", "city")
)
