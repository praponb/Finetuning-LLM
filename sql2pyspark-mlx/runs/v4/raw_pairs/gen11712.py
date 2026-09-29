from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city") == "Singapore")
    .select("supplier_name")
    .unionByName(df_suppliers.filter(F.col("city").isNull()).select("supplier_name"))
)
