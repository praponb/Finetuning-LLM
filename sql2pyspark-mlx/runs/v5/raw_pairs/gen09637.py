from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "account_type").orderBy(F.col("opened_date").asc_nulls_first(), "id")

cleaned = (
    df_accounts
    .filter((F.col("opened_date") >= F.lit("2023-03-12").cast("date")) & F.col("status").isNotNull())
    .select("*", F.abs("balance").alias("abs_balance"))
)

ranked = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

eligible = (
    ranked
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") < F.col("s.opened_date")), "left_semi")
)

matched_keys = (
    eligible
    .select("status", "account_type")
    .exceptAll(
        df_accounts
        .filter(F.col("account_type") == "SAVINGS")
        .select("status", "account_type")
    )
)

reconciled = (
    eligible
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.account_type") == F.col("k.account_type")), "inner")
    .select("s.*")
)

joined = (
    reconciled
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.age") < 58)
    .select("a.*", F.col("c.email"), F.col("c.age"), F.col("c.credit_limit"))
)

result = (
    joined
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "left")
    .rollup(F.col("d.country").alias("country"), F.col("d.city").alias("city"))
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.col("a.age")).alias("total_age"),
    )
)
