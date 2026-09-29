from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isin([6, 17, 26]))
    .select("id", "country")
)
