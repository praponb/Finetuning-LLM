from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("country").isin(["JP", "TH", "DE"]) & (F.col("country") == "DE") & F.col("browser").isNull())
    .select(
        "id",
        "customer_id",
        "started_at",
        F.quarter("started_at").alias("started_at_quarter"),
    )
)
