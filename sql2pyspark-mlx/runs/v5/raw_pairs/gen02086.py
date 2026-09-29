from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.partitionBy("status", "account_type")
w3 = Window.orderBy(F.col("balance").desc())
w4 = Window.partitionBy("account_type").orderBy(F.col("age").desc_nulls_last())
w5 = Window.orderBy(F.col("age_share").asc_nulls_last())
w6 = Window.partitionBy("account_type")
w7 = Window.partitionBy("account_type_count_age_tier").orderBy(F.col("open_status_min_balance").desc())

kept_keys = (
    df_accounts
    .select("customer_id", "status")
    .intersectAll(
        df_accounts
        .filter(F.col("customer_id").isNotNull())
        .select("customer_id", "status")
    )
)

kept_rows = (
    df_accounts
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

joined = (
    kept_rows
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id").eqNullSafe(F.col("c.id")), "inner")
    .select(
        "a.customer_id",
        "a.account_type",
        "a.balance",
        "a.status",
        F.col("c.email"),
        F.col("c.age"),
        F.col("c.credit_limit"),
    )
)

scored = (
    joined
    .filter(F.col("credit_limit") >= 13700)
    .select(
        "account_type",
        "balance",
        "status",
        "email",
        "age",
        F.round((F.col("age") / F.sum("age").over(w)), 4).alias("age_share"),
    )
)

scored_accounts = (
    scored
    .select(
        "*",
        F.min("balance").over(w2).alias("status_min_balance"),
        F.dense_rank().over(w3).alias("balance_dense_rank"),
        F.dense_rank().over(w4).alias("age_dense_rank"),
        F.rank().over(w5).alias("age_share_rank"),
        F.count("age").over(w6).alias("account_type_count_age"),
    )
)

filtered = (
    scored_accounts
    .filter(F.col("status").isNotNull())
    .select(
        "*",
        F.least("age", "balance_dense_rank").alias("least_age_balance_dense_rank"),
        F.coalesce("status", F.lit("OTHER")).alias("status_clean"),
    )
)

grouped = (
    filtered
    .groupBy(
        F.when(F.col("account_type_count_age") >= 16, "HIGH").otherwise("LOW").alias("account_type_count_age_tier"),
        F.when(F.col("balance_dense_rank") >= 7, "LARGE").otherwise("SMALL").alias("balance_dense_rank_tier"),
    )
    .agg(
        F.size(F.collect_set("status")).alias("num_status"),
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.round(F.avg("balance_dense_rank"), 2).alias("avg_balance_dense_rank"),
        F.round(F.sum("balance") / F.nullif(F.count(F.when(F.col("status") == "CLOSED", 1)), F.lit(0)), 2).alias("balance_per_closed"),
        F.sum(F.when(F.col("status") == "OPEN", F.col("status_min_balance")).otherwise(0)).alias("open_status_min_balance"),
    )
    .withColumn("open_status_min_balance_percent_rank", F.percent_rank().over(w7))
)

result = (
    grouped
    .select(
        "account_type_count_age_tier",
        "balance_dense_rank_tier",
        "num_status",
        "balance_quartiles",
        "avg_balance",
        "avg_balance_dense_rank",
        "balance_per_closed",
        "open_status_min_balance",
        "open_status_min_balance_percent_rank",
    )
)
