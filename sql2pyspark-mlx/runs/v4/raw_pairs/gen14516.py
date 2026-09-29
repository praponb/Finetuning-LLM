from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type", "merchant").orderBy("txn_time", "id")

deduped = (
    df_transactions
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

enriched = (
    deduped
    .alias("s")
    .join(df_accounts.alias("p"), F.col("s.account_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.status"), F.col("p.account_type"))
)

joined = (
    enriched
    .alias("a")
    .join(df_accounts.alias("b"), F.col("a.account_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.opened_date"))
)

result = (
    joined
    .groupBy(
        F.when(F.col("amount") >= 4000, "HIGH").otherwise("LOW").alias("amount_tier"),
        "account_id",
    )
    .agg(
        F.percentile_approx("amount", 0.9).alias("amount_p90"),
        F.max("txn_time").alias("last_txn_time"),
    )
    .filter(F.col("amount_p90") >= 0)
    .orderBy("account_id")
)
