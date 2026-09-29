from pyspark.sql import functions as F

filtered = (
    df_shipments
    .filter(F.col("shipped_date") > F.lit("2023-08-15").cast("date"))
    .withColumn("status_title", F.initcap(F.trim("status")))
    .withColumn("cost_filled", F.coalesce("cost", F.lit(0)))
)

result = (
    filtered
    .groupBy("carrier")
    .pivot("status", ["LOST", "IN_TRANSIT", "DELIVERED"])
    .agg(F.avg("cost_filled").alias("amount"))
)
