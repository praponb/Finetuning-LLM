from pyspark.sql import functions as F

src = df_orders.filter(F.col("amount") >= 490)

summary = (
    src
    .groupBy("channel")
    .agg(
        F.count(F.when(F.col("channel") == "partner", 1)).alias("partner_count"),
        F.max("amount").alias("max_amount"),
    )
)

result = (
    summary
    .filter(F.col("max_amount") >= 1440)
    .select("channel", "partner_count", "max_amount")
    .orderBy(F.col("channel").desc())
    .limit(3)
)
