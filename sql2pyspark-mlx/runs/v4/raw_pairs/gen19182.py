from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("merchant_length").desc())
w2 = Window.partitionBy("account_id").orderBy("balance")

base = (
    df_transactions
    .filter((F.col("txn_time") >= F.lit("2024-01-23").cast("date")) & F.col("merchant").isNotNull())
    .withColumn("merchant_length", F.length("merchant"))
    .withColumn("merchant_clean", F.coalesce("merchant", F.lit("N/A")))
)

joined = (
    base
    .alias("src")
    .join(df_accounts.alias("d"), F.col("src.account_id") == F.col("d.id"), "inner")
    .select("src.*", F.col("d.opened_date"), F.col("d.balance"), F.col("d.status"))
)

enriched = (
    joined
    .select(
        "*",
        F.dense_rank().over(w).alias("merchant_length_dense_rank"),
        F.dense_rank().over(w2).alias("balance_dense_rank"),
    )
)

summary = (
    enriched
    .groupBy(F.date_trunc("YEAR", "txn_time").alias("year_start"))
    .agg(
        F.avg(F.when(F.col("merchant") == "Amazon", F.col("merchant_length_dense_rank"))).alias("amazon_avg_merchant_length_dense_rank"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
        F.sum(F.when(F.col("txn_type") == "FEE", F.col("amount")).otherwise(0)).alias("fee_amount"),
        F.sum("amount").alias("total_amount"),
    )
)

result = (
    summary
    .select(
        "year_start",
        "amazon_avg_merchant_length_dense_rank",
        "fee_amount",
        "total_amount",
    )
    .orderBy(F.col("year_start").desc(), "fee_amount")
    .limit(10)
)
