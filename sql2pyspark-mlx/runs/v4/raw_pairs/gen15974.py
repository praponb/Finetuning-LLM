from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser").isin(["Edge", "Firefox"]) & F.col("customer_id").isin([2, 24]) & (F.col("device") == "mobile"))
    .select(
        "id",
        "device",
        "browser",
        F.substring("device", 1, 2).alias("device_prefix"),
    )
)
