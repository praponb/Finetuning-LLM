from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(~F.col("country").isin(["SG", "TH", "UK"]))
    .select(
        "id",
        "device",
        "country",
        "started_at",
        F.when(F.col("browser").isin(["Chrome", "Safari"]), "Priority").otherwise("Other").alias("browser_group"),
    )
)
