from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").isNotNull())
    .select("city")
    .unionByName(df_suppliers.filter(F.col("city").isNull()).select("city"))
)
