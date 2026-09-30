from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.opened_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("account_type").orderBy(F.col("credit_limit").desc_nulls_last())
w_hist = Window.orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("balance", "id")
w5 = Window.partitionBy("account_type").orderBy("credit_limit", "id")
w_part = Window.partitionBy("account_type").orderBy("signup_date", "id")

with_latest = (
    df_customers
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.opened_date") <= F.col("s.signup_date")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.name",
        "s.email",
        "s.signup_date",
        "s.is_active",
        "s.credit_limit",
        F.col("c.account_type"),
        F.col("c.balance"),
    )
)

metrics = (
    with_latest
    .select(
        "*",
        F.dense_rank().over(w2).alias("credit_limit_dense_rank"),
        F.first("balance", ignorenulls=True).over(w_hist).alias("first_known_balance"),
        F.row_number().over(w4).alias("seq_no"),
    )
)

filtered = (
    metrics
    .filter((F.col("signup_date") >= F.lit("2023-08-19").cast("date")) & (F.col("balance") < 30000))
    .withColumn("email_prefix", F.substring("email", 1, 3))
    .withColumn("seq_no_band", F.when(F.col("seq_no") >= 7, "High").when(F.col("seq_no") >= 4, "Medium").otherwise("Low"))
    .withColumn("resolved_account_type", F.coalesce("account_type", F.lit("UNKNOWN")))
)

enriched = (
    filtered
    .select(
        "id",
        "name",
        "email",
        "is_active",
        "account_type",
        "balance",
        "credit_limit_dense_rank",
        "seq_no",
        "email_prefix",
        "resolved_account_type",
        F.lead("seq_no").over(w5).alias("next_seq_no"),
        F.lag("balance").over(w5).alias("prev_balance"),
        F.lead("balance").over(w5).alias("next_balance"),
        F.row_number().over(w5).alias("seq_no_2"),
        F.row_number().over(w_part).alias("event_seq"),
    )
)

not_flagged = (
    enriched
    .alias("s")
    .join(df_subscriptions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.year(F.col("x.start_date")) == 2025), "left_semi")
)

result = (
    not_flagged
    .groupBy("next_seq_no")
    .agg(
        F.count_if(F.col("account_type") == "SAVINGS").alias("savings_events"),
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
        F.count_if(F.col("account_type") == "CHECKING").alias("checking_events"),
    )
    .orderBy(F.col("savings_events").desc_nulls_last())
)
