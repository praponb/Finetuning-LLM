from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("%Glob%") | (F.upper(F.trim("supplier_name")) == "TOKYO TRADERS"))
    .select("id", "city", "supplier_name")
)
