from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") != "US")
    .select("country")
    .intersect(df_sessions.filter(F.col("device") != "mobile").select("country"))
    .orderBy("country")
)
