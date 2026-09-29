from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("balance", "id")
w_hist = Window.partitionBy("account_type", "customer_id").orderBy("balance")
w3 = Window.partitionBy("account_type")
w4 = Window.partitionBy("customer_id").orderBy(F.col("balance").desc_nulls_first())

enriched = (
    df_accounts
    .withColumn("balance_bucket", F.ntile(4).over(w))
    .withColumn("balance_dense_rank", F.dense_rank().over(w_hist))
)

matched_keys = (
    enriched
    .select("id", "account_type")
    .intersectAll(
        df_accounts
        .filter(F.col("opened_date") < "2024-08-01")
        .select("id", "account_type")
    )
)

verified = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.account_type") == F.col("k.account_type")), "inner")
    .select("s.*")
)

enriched_accounts = (
    verified
    .select(
        "*",
        F.count("balance").over(w3).alias("account_type_count_balance"),
        F.cume_dist().over(w4).alias("balance_cume_dist"),
        F.sum("balance").over(w3).alias("account_type_sum_balance"),
        F.round((F.col("balance_dense_rank") / F.sum("balance_dense_rank").over(w3)), 4).alias("balance_dense_rank_share"),
    )
)

result = (
    enriched_accounts
    .select(
        "id",
        "opened_date",
        "balance_dense_rank",
        "account_type_count_balance",
        "balance_cume_dist",
        "account_type_sum_balance",
        "balance_dense_rank_share",
    )
    .orderBy(F.col("balance_cume_dist").desc(), F.col("opened_date").desc())
)
