from pyspark.sql import functions as F

base = (
    df_accounts
    .filter((F.col("opened_date") >= F.lit("2024-02-02").cast("date")) & F.col("status").isNotNull())
    .select("*", F.ceil("balance").alias("balance_whole"))
)

enriched = (
    base
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .select("a.*", F.col("c.country"), F.col("c.email"))
)

kept_keys = (
    enriched
    .select("account_type", "id")
    .subtract(
        df_accounts
        .filter(F.col("customer_id") == 31)
        .select("account_type", "id")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.account_type") == F.col("k.account_type")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "id",
        "account_type",
        "opened_date",
        "status",
        "balance_whole",
        "country",
        F.when(F.col("status").rlike("^(OPEN|CLOSED)$"), "CORE").when(F.col("status").rlike("^(FROZEN)$"), "GROUP_A").otherwise("EXTENDED").alias("status_zone"),
    )
    .orderBy(F.col("balance_whole").asc_nulls_last())
)
