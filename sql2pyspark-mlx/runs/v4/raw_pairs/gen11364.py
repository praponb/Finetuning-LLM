from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("balance", "id")
w2 = Window.orderBy("balance", "id")
w_grp = Window.partitionBy("account_type", "customer_id").orderBy(F.col("balance").asc_nulls_last())
w_part = Window.partitionBy("status", "customer_id")
w_part_2 = Window.partitionBy("customer_id", "status").orderBy(F.col("balance").desc_nulls_first())
w6 = Window.partitionBy("account_type")
w7 = Window.partitionBy("status")
w8 = Window.partitionBy("account_type").orderBy(F.col("next_balance").desc_nulls_last())
w9 = Window.partitionBy("customer_id", "status").orderBy(F.col("next_balance").desc())
w10 = Window.partitionBy("status").orderBy(F.col("status_max_next_balance").desc())
w11 = Window.partitionBy("customer_id")
w_recent = Window.partitionBy("status").orderBy(F.col("status_sum_balance_bucket").desc_nulls_last())
w13 = Window.partitionBy("customer_id").orderBy(F.col("balance_cume_dist").desc_nulls_first())
w_recent_2 = Window.partitionBy("account_type", "status").orderBy(F.col("next_balance").desc_nulls_last())
w15 = Window.orderBy(F.col("credit_limit").desc_nulls_last())

scored = (
    df_accounts
    .withColumn("next_balance", F.lead("balance", 2).over(w))
    .withColumn("balance_bucket", F.ntile(10).over(w2))
    .withColumn("balance_cume_dist", F.cume_dist().over(w_grp))
)

joined = (
    scored
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.signup_date"), F.col("p.credit_limit"))
)

remaining_keys = (
    joined
    .select("account_type")
    .subtract(
        df_accounts
        .filter(F.col("status").isin(["FROZEN", "OPEN"]))
        .select("account_type")
    )
)

verified = (
    joined
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.account_type") == F.col("k.account_type")), "inner")
    .select("s.*")
)

with_windows = (
    verified
    .withColumn("status_sum_balance_bucket", F.sum("balance_bucket").over(w_part))
    .withColumn("balance_cume_dist_2", F.cume_dist().over(w_part_2))
    .withColumn("account_type_min_balance_cume_dist", F.min("balance_cume_dist").over(w6))
    .withColumn("status_max_next_balance", F.max("next_balance").over(w7))
    .withColumn("next_balance_rank", F.rank().over(w8))
)

with_dims = (
    with_windows
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .select(
        "a.*",
        F.col("c.name"),
        F.col("c.credit_limit").alias("customer_credit_limit"),
    )
)

scored_accounts = (
    with_dims
    .withColumn("next_balance_rank_2", F.rank().over(w9))
    .withColumn("status_min_customer_credit_limit", F.min("customer_credit_limit").over(w7))
    .withColumn("status_max_next_balance_rank", F.rank().over(w10))
)

result = (
    scored_accounts
    .select(
        "*",
        F.sum("status_max_next_balance").over(w11).alias("customer_id_sum_status_max_next_balance"),
        F.dense_rank().over(w_recent).alias("status_sum_balance_bucket_dense_rank"),
        F.rank().over(w13).alias("balance_cume_dist_rank"),
        F.percent_rank().over(w_recent_2).alias("next_balance_percent_rank"),
        F.dense_rank().over(w15).alias("credit_limit_dense_rank"),
    )
)
