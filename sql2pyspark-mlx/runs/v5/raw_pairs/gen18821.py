from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("balance_rounded").desc_nulls_first())
w2 = Window.partitionBy("account_type", "customer_id").orderBy("balance_rounded")
w3 = Window.partitionBy("customer_id", "account_type").orderBy(F.col("balance_rounded_dense_rank").desc_nulls_last())

filtered = (
    df_accounts
    .filter((F.col("opened_date") >= F.lit("2023-04-04").cast("date")) & F.month("opened_date").isin([5, 7, 9]))
    .select(
        "id",
        "customer_id",
        "account_type",
        "opened_date",
        F.round("balance", 2).alias("balance_rounded"),
    )
)

kept_keys = (
    filtered
    .select("customer_id", "account_type")
    .intersect(
        df_accounts
        .filter(F.year("opened_date") == 2023)
        .select("customer_id", "account_type")
    )
)

verified = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.account_type") == F.col("k.account_type")), "inner")
    .select("s.*")
)

enriched = verified.withColumn("balance_rounded_dense_rank", F.dense_rank().over(w))

with_windows = (
    enriched
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance_rounded",
        "balance_rounded_dense_rank",
        F.rank().over(w2).alias("balance_rounded_rank"),
        F.rank().over(w3).alias("balance_rounded_dense_rank_rank"),
    )
)

joined = (
    with_windows
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "inner")
    .select(
        "e.customer_id",
        "e.account_type",
        "e.balance_rounded_dense_rank",
        "e.balance_rounded_rank",
        F.col("c.email"),
        F.col("c.name"),
    )
)

filtered_accounts = (
    joined
    .filter(F.col("name").like("%MILL%"))
    .select(
        "customer_id",
        "balance_rounded_rank",
        "email",
        "name",
        (F.col("balance_rounded_rank") + F.col("balance_rounded_dense_rank")).alias("balance_rounded_rank_plus_balance_rounded_dense_rank"),
    )
)

result = (
    filtered_accounts
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("cnt"),
        F.count("*").alias("event_count"),
        F.round(F.avg("balance_rounded_rank"), 2).alias("avg_balance_rounded_rank"),
        F.round(F.avg("balance_rounded_rank"), 2).alias("avg_balance_rounded_rank_2"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.count("*").alias("cnt_2"),
    )
    .orderBy(F.col("cnt").desc_nulls_last())
)
