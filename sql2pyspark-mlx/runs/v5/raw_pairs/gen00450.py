from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("country").isNotNull())
    .select(
        "customer_id",
        "browser",
        F.initcap(F.trim("device")).alias("device_title"),
        F.lower("browser").alias("browser_lower"),
    )
    .orderBy("customer_id")
)
