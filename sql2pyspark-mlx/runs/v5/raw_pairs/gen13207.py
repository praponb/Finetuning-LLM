from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("amount") > 950) & F.col("order_id").isNotNull())
    .select("id", "method", F.round("amount", 1).alias("amount_rounded"))
    .orderBy(F.col("method").desc(), F.col("id").desc())
    .limit(10)
)
