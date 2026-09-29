from pyspark.sql import functions as F

base = (
    df_accounts
    .filter(F.col("opened_date").between("2023-06-30", "2024-05-01"))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.date_add("opened_date", 89).alias("opened_date_plus_89d"),
        F.datediff(F.current_date(), "opened_date").alias("days_since_opened_date"),
        F.coalesce("status", F.lit("UNKNOWN")).alias("resolved_status"),
    )
)

not_flagged = (
    base
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") >= F.col("s.opened_date_plus_89d")), "left_anti")
)

enriched = (
    not_flagged
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "left")
    .select("e.*", F.col("c.city"), F.col("c.age"))
)

filtered = (
    enriched
    .filter(F.col("age") == 48)
    .withColumn("is_berlin", F.when(F.col("city") == "Berlin", 1).otherwise(0))
    .withColumn("city_prefix", F.substring("city", 1, 3))
)

result = (
    filtered
    .groupBy("status", "customer_id")
    .pivot("account_type", ["SAVINGS", "CREDIT", "LOAN"])
    .agg(F.avg("balance"))
)
