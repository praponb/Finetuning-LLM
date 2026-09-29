from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit") > 6100)
    .select("country")
    .intersect(df_sessions.select("country"))
)
