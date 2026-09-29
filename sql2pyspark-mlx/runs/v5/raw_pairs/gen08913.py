from pyspark.sql import functions as F

src = (
    df_payments
    .filter(F.col("method").isin(["card", "paypal"]) & (F.col("amount") > 1120) & F.col("order_id").isNotNull())
    .withColumn("method_group", F.when(F.col("method").isin(["bank_transfer", "cash"]), "Tier 1").otherwise("Other"))
)

summary = (
    src
    .groupBy(F.quarter("paid_at").alias("paid_quarter"))
    .agg(F.count(F.when(F.col("status") == "FAILED", 1)).alias("failed_count"))
)

result = summary.select("paid_quarter", "failed_count")
