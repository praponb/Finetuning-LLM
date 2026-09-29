from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "city")
w2 = Window.orderBy("balance")
w_ordered = Window.partitionBy("city", "status").orderBy(F.col("balance").desc_nulls_last())
w_part = Window.partitionBy("customer_id")

kept_keys = (
    df_accounts
    .select("customer_id", "id")
    .subtract(
        df_accounts
        .filter(F.col("opened_date") < "2023-11-01")
        .select("customer_id", "id")
    )
)

verified = (
    df_accounts
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    verified
    .filter((F.col("opened_date") >= F.lit("2023-08-16").cast("date")) & (F.year("opened_date") >= 2025))
    .withColumn("account_type_status", F.concat("account_type", F.lit(" "), "status"))
    .withColumn("resolved_status", F.coalesce("status", F.lit("N/A")))
)

cleaned = (
    valid
    .filter((F.col("opened_date") >= F.to_date(F.lit("2023-06-14"))) & F.col("status").isNotNull())
    .select(
        "*",
        F.trim("account_type").alias("clean_account_type"),
        F.date_sub("opened_date", 26).alias("opened_date_minus_26d"),
        F.coalesce("status", F.lit("OTHER")).alias("status_clean"),
    )
)

joined = (
    cleaned
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "inner")
    .filter(F.col("b.age").between(55, 56))
    .select("s.*", F.col("b.segment"), F.col("b.city"))
)

kept = (
    joined
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date_minus_26d")), "left_anti")
)

with_windows = (
    kept
    .select(
        "id",
        "account_type",
        "opened_date",
        "status",
        "account_type_status",
        "resolved_status",
        "opened_date_minus_26d",
        "status_clean",
        "segment",
        "city",
        F.avg("balance").over(w).alias("status_avg_balance"),
        F.cume_dist().over(w2).alias("balance_cume_dist"),
        F.percent_rank().over(w_ordered).alias("balance_percent_rank"),
        F.avg("balance").over(w_part).alias("customer_id_avg_balance"),
        F.sum("balance").over(w_part).alias("customer_id_sum_balance"),
    )
)

eligible = (
    with_windows
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_type") == "CREDIT"), "left_semi")
)

result = (
    eligible
    .groupBy(
        F.when(F.col("customer_id_sum_balance") >= 407089, "HIGH").otherwise("LOW").alias("customer_id_sum_balance_tier"),
    )
    .agg(
        F.count(F.when(F.col("account_type") == "CREDIT", 1)).alias("credit_count"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
    .orderBy(F.col("credit_count").desc_nulls_first())
)
