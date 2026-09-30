from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.lower("supplier_name").like("%lob%"))
    .select("id", "city")
    .subtract(df_suppliers.filter(F.col("city") != "New York").select("id", "city"))
)
