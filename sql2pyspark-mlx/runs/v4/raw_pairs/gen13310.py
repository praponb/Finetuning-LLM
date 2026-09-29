from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isNotNull())
    .select("id", "device")
    .intersect(
        df_sessions
        .filter(F.col("device").isin(["tablet", "mobile"]))
        .select("id", "device")
    )
)
