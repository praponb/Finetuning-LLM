from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("Si%"))
    .select("country")
    .unionByName(df_sessions.select("country"))
    .distinct()
)
