from pyspark.sql import functions as F

matched_keys = (
    df_accounts
    .select("account_type")
    .intersectAll(
        df_accounts
        .filter(F.col("status") == "FROZEN")
        .select("account_type")
    )
)

verified = (
    df_accounts
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.account_type") == F.col("k.account_type")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.when(F.col("balance") >= 69000, "HIGH").otherwise("LOW").alias("balance_tier"),
        "status",
    )
    .agg(
        F.count(F.when(F.col("account_type") == "CHECKING", 1)).alias("checking_count"),
        F.count(F.when(F.col("account_type") == "CREDIT", 1)).alias("credit_count"),
        F.count_if(F.col("status") == "OPEN").alias("open_events"),
    )
)
