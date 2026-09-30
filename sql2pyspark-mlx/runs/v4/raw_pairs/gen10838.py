from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(((F.col("device") == "mobile") & F.col("customer_id").isin([12, 29, 32])) | F.col("country").isin(["SG", "JP"]))
    .select(
        "id",
        "browser",
        "customer_id",
        "device",
        F.concat_ws(",", "device", "country").alias("device_country_key"),
    )
    .orderBy("device", "id")
    .limit(1)
)
