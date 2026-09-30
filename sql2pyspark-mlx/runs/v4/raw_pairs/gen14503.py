from pyspark.sql import functions as F

amount_by_customer_id = df_orders.groupBy("customer_id").agg(F.max("amount").alias("max_order_amount"))

with_dims = (
    df_accounts
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.credit_limit"), F.col("p.city"), F.col("p.email"))
)

matched_keys = (
    with_dims
    .select("status", "account_type")
    .subtract(
        df_accounts
        .filter(F.col("status").isin(["FROZEN", "CLOSED", "OPEN"]))
        .select("status", "account_type")
    )
)

reconciled = (
    with_dims
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.account_type") == F.col("k.account_type")), "inner")
    .select("s.*")
)

kept = (
    reconciled
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.merchant").isin(["7-Eleven", "Lazada", "Amazon"])), "left_anti")
)

daily = (
    kept
    .rollup("customer_id", F.date_trunc("WEEK", "opened_date").alias("week_start"))
    .agg(
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
        F.max(F.when(F.col("status") == "FROZEN", F.col("credit_limit"))).alias("frozen_max_credit_limit"),
        F.percentile_approx("balance", 0.5).alias("balance_p50"),
        F.round(F.sum("credit_limit") / F.nullif(F.count(F.when(F.col("account_type") == "CHECKING", 1)), F.lit(0)), 2).alias("credit_limit_per_checking"),
    )
    .filter(F.col("credit_limit_per_checking") > 0)
)

result = (
    daily
    .join(amount_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "frozen_max_credit_limit",
        "balance_p50",
        "credit_limit_per_checking",
        F.col("max_order_amount"),
    )
    .orderBy(F.col("customer_id").desc(), F.col("credit_limit_per_checking").desc())
)
