from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.lower(F.trim("supplier_name")) == "global supply")
    .select("id", "rating", F.lower("supplier_name").alias("supplier_name_lower"))
)
