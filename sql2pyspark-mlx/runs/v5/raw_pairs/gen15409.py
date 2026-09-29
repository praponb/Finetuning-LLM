from pyspark.sql import functions as F

result = (
    df_sessions
    .select("id", "customer_id")
    .intersect(
        df_sessions
        .filter(F.col("browser") == "Edge")
        .select("id", "customer_id")
    )
)
