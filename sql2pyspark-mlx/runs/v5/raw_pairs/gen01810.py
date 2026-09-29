from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser").isin(["Firefox", "Chrome"]))
    .select("country", "customer_id")
    .unionByName(
        df_sessions
        .filter(F.col("started_at") < "2023-07-01")
        .select("country", "customer_id")
    )
    .distinct()
)
