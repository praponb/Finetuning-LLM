from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "account_type").orderBy(F.col("opened_date").desc_nulls_first(), "id")
w2 = Window.partitionBy("balance_adjusted_tier").orderBy(F.col("open_rows").desc())
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.avg("balance").alias("avg_account_balance"))

filtered = (
    df_accounts
    .filter(F.col("status").isNotNull())
    .select(
        "*",
        F.concat_ws("-", "status", "account_type").alias("status_account_type_key"),
        F.coalesce("status", F.lit("NONE")).alias("resolved_status"),
    )
)

ranked = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

remaining_keys = (
    ranked
    .select("id", "account_type")
    .exceptAll(
        df_accounts
        .filter(F.col("customer_id") == 34)
        .select("id", "account_type")
    )
)

reconciled = (
    ranked
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.account_type") == F.col("k.account_type")), "inner")
    .select("s.*")
)

filtered_accounts = (
    reconciled
    .filter((F.col("opened_date") > F.lit("2023-05-18").cast("date")) & F.col("status").isin(["OPEN", "FROZEN"]))
    .withColumn("balance_adjusted", F.round((F.col("balance") * 1.1), 2))
    .withColumn("status_filled", F.coalesce("status", F.lit("NONE")))
)

totals = (
    filtered_accounts
    .groupBy(
        F.when(F.col("balance_adjusted") >= 81, "LARGE").otherwise("SMALL").alias("balance_adjusted_tier"),
        "customer_id",
    )
    .agg(
        F.count(F.when(F.col("status") == "OPEN", 1)).alias("open_rows"),
        F.countDistinct("account_type").alias("distinct_account_types"),
    )
    .filter(F.col("distinct_account_types") > 1)
    .withColumn("open_rows_dense_rank", F.dense_rank().over(w2))
)

result = (
    totals
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "balance_adjusted_tier",
        "customer_id",
        "open_rows",
        "distinct_account_types",
        "open_rows_dense_rank",
        F.col("avg_account_balance"),
    )
)
