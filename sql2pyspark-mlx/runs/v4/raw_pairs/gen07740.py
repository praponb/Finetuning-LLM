from pyspark.sql import functions as F

result = (
    df_sessions
    .select("browser", "customer_id")
    .intersect(
        df_sessions
        .filter(F.col("device") == "desktop")
        .select("browser", "customer_id")
    )
)
