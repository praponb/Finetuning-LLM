from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page").isNotNull() | F.col("page").isin(["/cart", "/product"]))
    .select(
        "id",
        "event_type",
        "session_id",
        "duration_sec",
        F.when(F.col("duration_sec") >= 512, "High").when(F.col("duration_sec") >= 176, "Medium").otherwise("Low").alias("duration_sec_tier"),
    )
)
