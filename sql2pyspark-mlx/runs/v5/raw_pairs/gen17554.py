from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "browser",
        F.to_date("started_at").alias("started_at_date"),
        F.when(F.col("device").isin(["mobile", "tablet"]), "Group A").otherwise("Other").alias("device_group"),
    )
)
