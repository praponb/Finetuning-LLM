from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit") >= 9800)
    .select("country")
    .subtract(df_sessions.select("country"))
)
