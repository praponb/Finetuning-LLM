from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("account_type_length").desc())
w2 = Window.partitionBy("customer_id", "status")
w3 = Window.partitionBy("account_type")
w4 = Window.partitionBy("customer_id").orderBy("account_type_length")
w5 = Window.partitionBy("customer_id", "account_type").orderBy(F.col("account_type_length").desc())

cleaned = (
    df_accounts
    .filter(F.col("opened_date") > "2025-10-01")
    .select(
        "*",
        F.length("account_type").alias("account_type_length"),
        F.concat_ws("-", "status", "account_type").alias("status_account_type_key"),
    )
)

matched_keys = (
    cleaned
    .select("id", "customer_id")
    .intersect(
        df_accounts
        .filter(F.col("account_type") == "LOAN")
        .select("id", "customer_id")
    )
)

kept_rows = (
    cleaned
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

kept = (
    kept_rows
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_type").isin(["DEBIT", "CREDIT"])), "left_anti")
)

eligible = (
    kept
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") >= F.col("s.opened_date")), "left_anti")
)

result = (
    eligible
    .select(
        "*",
        F.dense_rank().over(w).alias("account_type_length_dense_rank"),
        F.min("account_type_length").over(w2).alias("customer_id_min_account_type_length"),
        F.max("account_type_length").over(w3).alias("account_type_max_account_type_length"),
        F.cume_dist().over(w4).alias("account_type_length_cume_dist"),
        F.rank().over(w5).alias("account_type_length_rank"),
    )
)
