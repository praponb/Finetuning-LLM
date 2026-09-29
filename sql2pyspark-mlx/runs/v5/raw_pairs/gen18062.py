from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy(F.col("opened_date_period").desc(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("customer_id", "country").orderBy(F.col("credit_limit").asc_nulls_first())
w4 = Window.orderBy(F.col("opened_date_period").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w5 = Window.orderBy(F.col("opened_date_period").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w6 = Window.partitionBy("transaction_txn_type").orderBy(F.col("credit_limit").asc_nulls_first())

cleaned = (
    df_accounts
    .filter((F.col("opened_date") >= F.lit("2023-07-03").cast("date")) & F.col("status").isNotNull())
    .select(
        "id",
        "customer_id",
        "account_type",
        "opened_date",
        F.date_trunc("WEEK", "opened_date").alias("opened_date_period"),
        F.coalesce("status", F.lit("NONE")).alias("status_filled"),
    )
)

ranked = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "customer_id", "account_type", "opened_date_period", "status_filled")
)

latest_match = (
    ranked
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_type").alias("transaction_txn_type"))
)

remaining_keys = (
    latest_match
    .select("account_type", "id")
    .subtract(
        df_accounts
        .filter(F.col("account_type").isin(["CHECKING", "LOAN", "CREDIT"]))
        .select("account_type", "id")
    )
)

verified = (
    latest_match
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.account_type") == F.col("k.account_type")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.country"), F.col("p.credit_limit"))
)

not_flagged = (
    enriched
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") < F.col("s.opened_date_period")), "left_anti")
)

enriched_accounts = (
    not_flagged
    .withColumn("credit_limit_percent_rank", F.percent_rank().over(w3))
    .withColumn("credit_limit_count_7d", F.count("credit_limit").over(w4))
    .withColumn("credit_limit_max_90d", F.max("credit_limit").over(w5))
    .withColumn("credit_limit_sum_7d", F.sum("credit_limit").over(w4))
    .withColumn("credit_limit_dense_rank", F.dense_rank().over(w6))
)

result = (
    enriched_accounts
    .groupBy(
        F.when(F.col("credit_limit_percent_rank") >= 0.87, "PREMIUM").otherwise("STANDARD").alias("credit_limit_percent_rank_tier"),
        F.date_trunc("quarter", "opened_date_period").alias("quarter_start"),
        F.when(F.col("credit_limit") >= 13900, "HIGH").otherwise("LOW").alias("credit_limit_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("transaction_txn_type") == "CREDIT", 1)).alias("credit_count"),
        F.percentile_approx("credit_limit_max_90d", [0.25, 0.5, 0.75]).alias("credit_limit_max_90d_quartiles"),
    )
    .orderBy(F.col("credit_limit_percent_rank_tier").asc_nulls_first())
)
