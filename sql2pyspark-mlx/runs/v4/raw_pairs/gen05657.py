from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("balance").desc_nulls_first(), "id")

ranked = (
    df_accounts
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

base = (
    ranked
    .filter(F.col("status").isin(["CLOSED", "OPEN"]))
    .withColumn("opened_date_month_end", F.last_day("opened_date"))
)

with_dims = (
    base
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "inner")
    .select("src.*", F.col("d.country"), F.col("d.credit_limit"), F.col("d.name"))
)

base_accounts = (
    with_dims
    .filter(F.col("country").isNotNull() & (F.col("balance") >= 58000))
    .select(
        "*",
        (F.col("credit_limit") + F.col("balance")).alias("credit_limit_plus_balance"),
        F.add_months("opened_date", 12).alias("opened_date_plus_months"),
    )
)

kept = (
    base_accounts
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.merchant") == "Grab"), "left_anti")
)

matched_keys = (
    kept
    .select("customer_id")
    .exceptAll(
        df_accounts
        .filter(F.col("customer_id").isNull())
        .select("customer_id")
    )
)

verified = (
    kept
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .withColumn("credit_limit_plus_balance_tier", F.when(F.col("credit_limit_plus_balance") >= 81, "HIGH").otherwise("LOW"))
    .withColumn("week_start", F.date_trunc("WEEK", "opened_date_month_end"))
    .groupingSets(
        [["credit_limit_plus_balance_tier", "week_start"], ["credit_limit_plus_balance_tier"]],
        "credit_limit_plus_balance_tier",
        "week_start",
    )
    .agg(
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
        F.avg(F.when(F.col("country") == "SG", F.col("credit_limit"))).alias("sg_avg_credit_limit"),
        F.min("credit_limit_plus_balance").alias("min_credit_limit_plus_balance"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy("credit_limit_plus_balance_tier")
)
