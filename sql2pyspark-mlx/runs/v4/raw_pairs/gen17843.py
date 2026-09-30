from pyspark.sql import functions as F

filtered = (
    df_payments
    .filter(F.col("order_id").isNotNull() & F.col("order_id").isin([2, 18, 23]))
    .withColumn("amount_whole", F.ceil("amount"))
    .withColumn("amount_rounded", F.round("amount", 1))
)

result = (
    filtered
    .select("id", "status", "amount", "amount_rounded")
    .unpivot(["id", "status"], ["amount", "amount_rounded"], "kpi", "reading")
    .filter(F.col("reading").isNotNull())
)
