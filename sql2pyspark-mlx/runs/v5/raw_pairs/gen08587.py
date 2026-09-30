from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(~F.col("country").isin(["DE", "JP"]))
    .select(
        "id",
        "country",
        "customer_id",
        "device",
        F.when(F.col("browser").isin(["Chrome", "Safari"]), "Priority").otherwise("Other").alias("browser_group"),
    )
    .orderBy(F.col("id").desc())
    .limit(5)
)
