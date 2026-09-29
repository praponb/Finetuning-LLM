from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.hour("started_at").between(11, 19))
    .select(
        "id",
        "country",
        "customer_id",
        "browser",
        F.when(F.col("device").isin(["desktop", "tablet"]), "Tier 1").otherwise("Other").alias("device_group"),
    )
    .orderBy(F.col("browser").desc(), "id")
    .limit(20)
)
