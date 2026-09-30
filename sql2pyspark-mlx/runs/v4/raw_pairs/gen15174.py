from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("started_at") >= "2025-04-25")
    .select("browser", "country")
    .unionByName(
        df_sessions
        .filter(F.col("customer_id").isin([1, 19]))
        .select("browser", "country")
    )
    .distinct()
)
