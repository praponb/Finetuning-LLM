from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("country").isin(["JP", "UK"]) & F.hour("started_at").between(7, 15)) | (F.col("country") == "DE"))
    .select(
        "id",
        "device",
        "country",
        "customer_id",
        F.date_format("started_at", "yyyy-MM-dd").alias("started_at_label"),
    )
    .orderBy("device")
)
