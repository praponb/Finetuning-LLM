from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(~F.col("device").isin(["desktop", "tablet", "mobile"]) & (F.col("browser") != "Firefox"))
    .select(
        "id",
        "browser",
        "customer_id",
        "country",
        F.trim("device").alias("clean_device"),
    )
    .orderBy("country", "id")
    .limit(5)
)
