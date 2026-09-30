from pyspark.sql import functions as F

discount_by_customer_id = df_orders.groupBy("customer_id").agg(F.min("discount").alias("min_order_discount"))

valid = (
    df_accounts
    .filter(F.col("customer_id").isNull())
    .select("*", F.date_format("opened_date", "MMM yyyy").alias("opened_date_label"))
)

not_flagged = (
    valid
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.account_id").isin([6, 29])), "left_semi")
)

matched_keys = (
    not_flagged
    .select("account_type", "id")
    .intersect(
        df_accounts
        .filter(F.col("customer_id").isin([1, 8, 16, 30]))
        .select("account_type", "id")
    )
)

kept_rows = (
    not_flagged
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.account_type") == F.col("k.account_type")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

kept = (
    kept_rows
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") >= F.col("s.opened_date")), "left_anti")
)

base = (
    kept
    .filter(F.col("status").isin(["FROZEN", "OPEN", "CLOSED"]))
    .select(
        "customer_id",
        "account_type",
        "opened_date",
        "status",
        "opened_date_label",
        F.substring("account_type", 1, 3).alias("account_type_prefix"),
    )
)

cleaned = (
    base
    .filter(F.col("status").isNotNull())
    .withColumn("opened_date_month", F.month("opened_date"))
)

result = (
    cleaned
    .join(discount_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "account_type",
        "opened_date",
        "account_type_prefix",
        "opened_date_month",
        F.when(F.col("account_type").rlike("^(CHECKING|CREDIT)$"), "EXTENDED").otherwise("GROUP_A").alias("account_type_zone"),
        F.col("min_order_discount"),
    )
    .orderBy(F.col("opened_date_month").asc_nulls_last())
)
