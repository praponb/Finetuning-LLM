from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("opened_date").desc(), "id")
w2 = Window.partitionBy("country").orderBy(F.col("balance").desc(), "id")
w3 = Window.partitionBy("customer_id").orderBy(F.col("event_seq").desc_nulls_last())

joined = (
    df_accounts
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.country"))
)

metrics = (
    joined
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("balance_bucket", F.ntile(3).over(w2))
)

deduped = (
    metrics
    .withColumn("rn", F.rank().over(w3))
    .filter(F.col("rn") == 1)
)

daily = (
    deduped
    .groupBy(
        F.when(F.col("balance_bucket") >= 9, "PREMIUM").otherwise("STANDARD").alias("balance_bucket_tier"),
    )
    .agg(
        F.sum(F.when(F.col("account_type") == "LOAN", F.col("event_seq")).otherwise(0)).alias("loan_event_seq"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
    )
    .filter(F.col("loan_event_seq") >= 0)
)

result = (
    daily
    .select("balance_bucket_tier", "avg_balance")
    .orderBy(F.col("balance_bucket_tier").asc_nulls_last())
)
