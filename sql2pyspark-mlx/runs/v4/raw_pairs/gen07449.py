from pyspark.sql import functions as F

cleaned = (
    df_customers
    .filter(F.col("segment").isNotNull())
    .select(
        "*",
        F.upper(F.trim("email")).alias("email_normalized"),
        F.coalesce("city", F.lit("DIRECT")).alias("resolved_city"),
    )
)

result = (
    cleaned
    .groupBy("city", "country")
    .pivot("segment", ["SMB", "Consumer", "Corporate"])
    .agg(F.avg("credit_limit").alias("total"), F.count(F.lit(1)).alias("n"))
)
