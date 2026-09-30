from pyspark.sql import functions as F

filtered = (
    df_customers
    .filter((~F.col("segment").eqNullSafe(F.lit("Consumer"))) & (F.col("signup_date") >= F.lit("2023-03-09").cast("date")))
    .select(
        "*",
        F.when(F.col("segment").isin(["Consumer", "SMB"]), "Priority").otherwise("Other").alias("segment_group"),
    )
)

result = (
    filtered
    .groupBy("city")
    .pivot("country", ["TH", "JP"])
    .agg(F.avg("credit_limit"))
    .select("city", F.col("TH").alias("th"), F.col("JP").alias("jp"))
)
