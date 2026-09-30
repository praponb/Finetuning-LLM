from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type", "merchant").orderBy(F.col("opened_date").desc())
w2 = Window.partitionBy("txn_type").orderBy("txn_time_period")

valid = (
    df_transactions
    .filter(F.col("txn_time") > F.to_timestamp(F.lit("2024-01-07 01:00:00")))
    .withColumn("txn_time_period", F.date_trunc("YEAR", "txn_time"))
)

joined = (
    valid
    .alias("src")
    .join(df_accounts.alias("a"), F.col("src.account_id") == F.col("a.id"), "left")
    .select("src.*", F.col("a.opened_date"))
)

ranked = (
    joined
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

latest = (
    ranked
    .withColumn("row_num", F.rank().over(w2))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest
    .groupBy(
        F.when(F.col("amount") >= 3300, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
    )
    .agg(
        F.count_if(F.col("merchant") == "Amazon").alias("amazon_events"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
        F.percentile_approx("amount", 0.5).alias("amount_p50"),
    )
    .orderBy(F.col("amount_tier").asc_nulls_first())
)
