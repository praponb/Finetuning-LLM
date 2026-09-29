from pyspark.sql import functions as F

filtered = (
    df_accounts
    .filter((F.col("opened_date") >= F.to_date(F.lit("2023-03-03"))) & (F.col("account_type") == "LOAN"))
    .select(
        "*",
        F.when(F.col("account_type").isin(["LOAN", "SAVINGS"]), "Tier 1").otherwise("Other").alias("account_type_group"),
    )
)

kept_keys = (
    filtered
    .select("id")
    .subtract(
        df_accounts
        .filter(F.month("opened_date").isin([3, 7, 12]))
        .select("id")
    )
)

verified = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

not_flagged = (
    verified
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.to_date(F.col("c.txn_time")) < "2024-06-01"), "left_anti")
)

with_dims = (
    not_flagged
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "inner")
    .filter(F.col("p.name").like("D%"))
    .select("s.*", F.col("p.email"), F.col("p.segment"), F.col("p.age"))
)

result = (
    with_dims
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupingSets(
        [["country", "customer_segment", "account_segment"], ["country"], []],
        F.col("c.country").alias("country"),
        F.col("c.segment").alias("customer_segment"),
        F.col("s.segment").alias("account_segment"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.sum(F.col("s.age")).alias("total_age"),
    )
)
