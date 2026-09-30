from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.upper("supplier_name").like("%ACI%"))
    .select("city")
    .intersect(df_stores.select("city"))
    .orderBy(F.col("city").desc())
)
