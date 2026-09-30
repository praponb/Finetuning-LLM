from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(~(F.col("customer_id").isNotNull() | (F.col("customer_id") == 32)))
    .select(
        "id",
        "customer_id",
        "browser",
        F.initcap(F.trim("device")).alias("device_title"),
    )
    .orderBy("customer_id", "browser")
)
