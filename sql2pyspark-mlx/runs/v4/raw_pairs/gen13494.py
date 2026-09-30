from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id", "merchant").orderBy("opened_date", "id")
w2 = Window.partitionBy("quarter_start").orderBy(F.col("num_merchants").desc())

base = (
    df_transactions
    .filter((F.col("txn_time") > F.to_date(F.lit("2023-12-21"))) & F.col("merchant").isNotNull() & (F.col("merchant") == "Lazada"))
    .withColumn("txn_type_length", F.length("txn_type"))
)

enriched = (
    base
    .alias("src")
    .join(df_accounts.alias("a"), F.col("src.account_id") == F.col("a.id"), "left")
    .select("src.*", F.col("a.account_type"))
)

joined = (
    enriched
    .alias("s")
    .join(df_accounts.alias("d"), F.col("s.account_id") == F.col("d.id"), "left")
    .select(
        "s.*",
        F.col("d.account_type").alias("account_account_type"),
        F.col("d.opened_date"),
    )
)

ranked = (
    joined
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

enriched_transactions = (
    ranked
    .alias("s")
    .join(df_accounts.alias("a"), F.col("s.account_id") == F.col("a.id"), "left")
    .select("s.*", F.col("a.balance"), F.col("a.status"))
)

valid = (
    enriched_transactions
    .filter(F.col("account_type") == "CHECKING")
    .withColumn("amount_band", F.when(F.col("amount") >= 4400, "High").when(F.col("amount") >= 900, "Medium").otherwise("Low"))
)

remaining_keys = (
    valid
    .select("txn_type", "id")
    .exceptAll(
        df_transactions
        .filter(~F.col("merchant").isin(["Starbucks", "Amazon"]))
        .select("txn_type", "id")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.txn_type") == F.col("k.txn_type")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(
        F.date_trunc("QUARTER", "opened_date").alias("quarter_start"),
        F.when(F.col("txn_type_length") >= 85, "LARGE").otherwise("SMALL").alias("txn_type_length_tier"),
    )
    .agg(
        F.countDistinct("account_type").alias("distinct_account_types"),
        F.count("*").alias("num_rows"),
        F.size(F.collect_set("merchant")).alias("num_merchants"),
        F.count(F.when(F.col("account_account_type") == "LOAN", 1)).alias("loan_count"),
        F.stddev_samp("amount").alias("amount_stddev"),
    )
    .withColumn("num_merchants_rank", F.rank().over(w2))
    .orderBy(F.col("amount_stddev").desc())
)
