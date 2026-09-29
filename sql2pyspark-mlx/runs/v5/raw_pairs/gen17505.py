from pyspark.sql import functions as F

result = (
    df_sessions
    .select("browser", "customer_id")
    .subtract(
        df_sessions
        .filter(F.col("device") == "tablet")
        .select("browser", "customer_id")
    )
)
