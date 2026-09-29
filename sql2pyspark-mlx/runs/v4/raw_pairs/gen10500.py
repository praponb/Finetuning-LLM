from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("balance", "id")
w2 = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy("opened_date_minus_47d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("status").orderBy(F.col("running_balance").asc_nulls_first())
w6 = Window.partitionBy("account_type").orderBy("transaction_amount")
w7 = Window.partitionBy("status").orderBy("opened_date", "id").rowsBetween(-5, -1)

cleaned = (
    df_accounts
    .filter(F.col("status").isNotNull() & (F.col("account_type") == "CREDIT"))
    .withColumn("opened_date_minus_47d", F.date_sub("opened_date", 47))
)

cleaned_accounts = (
    cleaned
    .filter((F.col("opened_date") >= F.lit("2023-08-07").cast("date")) & F.col("status").isNotNull() & F.col("opened_date_minus_47d").between("2023-08-01", "2024-08-18"))
    .select(
        "*",
        F.quarter("opened_date_minus_47d").alias("opened_date_minus_47d_quarter"),
        F.coalesce("status", F.lit("UNKNOWN")).alias("resolved_status"),
    )
)

ranked = (
    cleaned_accounts
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

enriched = (
    ranked
    .withColumn("running_balance", F.sum("balance").over(w2))
    .withColumn("next_opened_date_minus_47d_quarter", F.lead("opened_date_minus_47d_quarter", 2).over(w))
    .withColumn("last_known_opened_date_minus_47d_quarter", F.last("opened_date_minus_47d_quarter", ignorenulls=True).over(w3))
)

with_latest = (
    enriched
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.amount").alias("transaction_amount"))
)

scored = (
    with_latest
    .withColumn("running_balance_cume_dist", F.cume_dist().over(w5))
    .withColumn("transaction_amount_dense_rank", F.dense_rank().over(w6))
    .withColumn("next_opened_date_minus_47d_quarter_avg_5_prior", F.avg("next_opened_date_minus_47d_quarter").over(w7))
)

result = (
    scored
    .groupBy("customer_id")
    .pivot("account_type", ["SAVINGS", "CREDIT"])
    .agg(
        F.avg("next_opened_date_minus_47d_quarter").alias("value_sum"),
        F.count(F.lit(1)).alias("cnt"),
    )
    .select(
        "customer_id",
        F.col("SAVINGS_value_sum").alias("savings_value_sum"),
        F.col("SAVINGS_cnt").alias("savings_cnt"),
        F.col("CREDIT_value_sum").alias("credit_value_sum"),
        F.col("CREDIT_cnt").alias("credit_cnt"),
    )
)
