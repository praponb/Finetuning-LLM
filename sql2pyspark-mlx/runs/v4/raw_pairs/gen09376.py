from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id") == 1)
    .select("device", "country")
    .unionByName(
        df_sessions
        .filter(F.col("started_at") < "2023-10-26")
        .select("device", "country")
    )
)
