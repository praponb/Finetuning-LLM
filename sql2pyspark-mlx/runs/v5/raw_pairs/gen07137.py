from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("amount_scaled").desc_nulls_first())
w_hist = Window.partitionBy("merchant").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w3 = Window.orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w4 = Window.partitionBy("merchant", "txn_type")
w5 = Window.partitionBy("txn_type").orderBy(F.col("amount_scaled").desc_nulls_last())
w6 = Window.orderBy(F.col("amount_scaled_max_30d").asc_nulls_first())
w7 = Window.orderBy("merchant_sum_amount_scaled")
w_running = Window.partitionBy("txn_type")
w_part = Window.partitionBy("txn_type").orderBy(F.col("amount_scaled_sum_7d").asc_nulls_first())
w10 = Window.partitionBy("account_id").orderBy(F.col("amount_scaled_sum_7d").asc_nulls_last())

valid = (
    df_transactions
    .filter(F.col("amount") < 3300)
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "merchant",
        F.round((F.col("amount") * 1.07), 2).alias("amount_scaled"),
        F.coalesce("merchant", F.lit("DIRECT")).alias("merchant_clean"),
    )
)

matched_keys = (
    valid
    .select("id", "account_id")
    .subtract(
        df_transactions
        .filter(F.col("txn_type").isin(["REFUND", "CREDIT", "DEBIT"]))
        .select("id", "account_id")
    )
)

verified = (
    valid
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.account_id") == F.col("k.account_id")), "inner")
    .select("s.*")
)

cleaned = (
    verified
    .filter((F.col("txn_time") > F.lit("2023-07-26 01:00:00").cast("timestamp")) & (~F.col("merchant").eqNullSafe(F.lit("7-Eleven"))))
    .select("*", F.initcap(F.trim("txn_type")).alias("txn_type_title"))
)

scored = (
    cleaned
    .select(
        "id",
        "account_id",
        "txn_type",
        "merchant_clean",
        F.dense_rank().over(w).alias("amount_scaled_dense_rank"),
        F.max("amount_scaled").over(w_hist).alias("amount_scaled_max_30d"),
        F.sum("amount_scaled").over(w3).alias("amount_scaled_sum_7d"),
        F.sum("amount_scaled").over(w4).alias("merchant_sum_amount_scaled"),
        F.percent_rank().over(w5).alias("amount_scaled_percent_rank"),
    )
)

enriched = (
    scored
    .filter(F.col("account_id").isin([1, 3, 33, 37]))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount_scaled_dense_rank",
        "amount_scaled_max_30d",
        "merchant_sum_amount_scaled",
        F.cume_dist().over(w6).alias("amount_scaled_max_30d_cume_dist"),
        F.dense_rank().over(w7).alias("merchant_sum_amount_scaled_dense_rank"),
        F.max("amount_scaled_max_30d").over(w_running).alias("txn_type_max_amount_scaled_max_30d"),
        F.cume_dist().over(w_part).alias("amount_scaled_sum_7d_cume_dist"),
        F.rank().over(w10).alias("amount_scaled_sum_7d_rank"),
    )
)

result = (
    enriched
    .alias("s")
    .join(df_accounts.alias("d"), F.col("s.account_id") == F.col("d.id"), "left")
    .groupingSets(
        [["status", "txn_type"], ["status"], []],
        F.col("d.status").alias("status"),
        F.col("s.txn_type").alias("txn_type"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.sum(F.col("s.amount_scaled_sum_7d_cume_dist")), 2).alias("total_amount_scaled_sum_7d_cume_dist"),
        F.round(F.avg(F.col("s.merchant_sum_amount_scaled_dense_rank")), 2).alias("avg_merchant_sum_amount_scaled_dense_rank"),
    )
)
