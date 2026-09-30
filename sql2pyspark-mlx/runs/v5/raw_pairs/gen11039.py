from pyspark.sql import functions as F

result = (
    df_sessions
    .select("customer_id", "device")
    .intersect(
        df_sessions
        .filter(F.hour("started_at").between(8, 12))
        .select("customer_id", "device")
    )
)
