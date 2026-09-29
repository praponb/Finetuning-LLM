from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("device") == "mobile") | F.col("browser").isin(["Safari", "Edge", "Firefox"]))
    .select(
        "id",
        "customer_id",
        "country",
        "device",
        F.when(F.col("device") == "mobile", 1).otherwise(0).alias("is_mobile"),
    )
)
