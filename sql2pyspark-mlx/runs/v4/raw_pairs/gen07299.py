from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "id",
        "customer_id",
        "country",
        F.length("device").alias("device_length"),
        F.when(F.col("browser").isin(["Safari", "Edge"]), "Tier 1").otherwise("Other").alias("browser_group"),
    )
)
