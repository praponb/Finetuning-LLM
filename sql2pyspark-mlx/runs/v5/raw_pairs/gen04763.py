from pyspark.sql import functions as F

result = (
    df_sessions
    .select("id", "customer_id")
    .orderBy(F.col("customer_id").desc(), "id")
    .limit(50)
)
