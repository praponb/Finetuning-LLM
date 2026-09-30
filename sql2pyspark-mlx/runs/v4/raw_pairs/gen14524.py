from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.hour("started_at").between(6, 11))
    .select("customer_id")
)
