from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("opened_date").desc(), "id")
w_hist = Window.partitionBy("customer_id").orderBy("opened_date", "id").rowsBetween(-5, -1)
w3 = Window.orderBy("balance", "id")
w4 = Window.partitionBy("customer_id").orderBy("balance", "id")
w5 = Window.partitionBy("customer_id").orderBy(F.col("balance").desc(), "id")
w6 = Window.orderBy(F.col("balance").desc(), "id")
w_grp = Window.orderBy("balance_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("customer_id").orderBy(F.col("balance_bucket_2").desc_nulls_last())
w9 = Window.partitionBy("customer_id")
w10 = Window.orderBy("balance_bucket_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

deduped = (
    df_accounts
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

valid = (
    deduped
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-10-29"))) & F.col("customer_id").isNotNull() & (F.col("opened_date") < "2025-09-18"))
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        F.substring("account_type", 1, 3).alias("account_type_prefix"),
        F.upper("account_type").alias("account_type_upper"),
    )
)

metrics = (
    valid
    .select(
        "*",
        F.avg("balance").over(w_hist).alias("balance_avg_5_prior"),
        F.row_number().over(w3).alias("seq_no"),
        F.ntile(3).over(w4).alias("balance_bucket"),
        F.ntile(3).over(w5).alias("balance_bucket_2"),
        F.row_number().over(w6).alias("seq_no_2"),
    )
)

kept = (
    metrics
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.merchant").isin(["7-Eleven", "Lazada", "Shell"])), "left_semi")
)

not_flagged = (
    kept
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") < F.col("s.opened_date")), "left_anti")
)

result = (
    not_flagged
    .withColumn("running_balance_bucket_2", F.sum("balance_bucket_2").over(w_grp))
    .withColumn("balance_bucket_2_cume_dist", F.cume_dist().over(w8))
    .withColumn("customer_id_avg_balance_bucket", F.avg("balance_bucket").over(w9))
    .withColumn("cumulative_seq_no", F.sum("seq_no").over(w10))
)
