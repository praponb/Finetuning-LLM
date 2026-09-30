from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") == "JP")
    .select("country")
    .unionByName(df_sessions.select("country"))
    .distinct()
)
