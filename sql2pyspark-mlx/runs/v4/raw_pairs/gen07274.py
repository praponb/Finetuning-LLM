from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "id",
        "device",
        F.when(F.col("browser").isin(["Chrome", "Firefox"]), "Core").otherwise("Other").alias("browser_group"),
    )
)
