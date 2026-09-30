from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser").isin(["Chrome", "Firefox"]) & ~F.col("country").isin(["SG", "US", "TH"]))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        F.to_date("started_at").alias("started_at_date"),
    )
)
