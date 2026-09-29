from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("amount").desc())

cleaned = (
    df_transactions
    .filter((F.col("txn_time") >= F.to_timestamp(F.lit("2023-12-11 09:00:00"))) & F.col("merchant").isNotNull())
    .select("*", F.upper("merchant").alias("merchant_upper"))
)

valid = (
    cleaned
    .filter(F.col("merchant").isNotNull() & F.hour("txn_time").between(11, 17))
    .withColumn("merchant_title", F.initcap(F.trim("merchant")))
)

ranked = (
    valid
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

valid_transactions = (
    ranked
    .filter(F.col("txn_time") > F.to_date(F.lit("2023-10-30")))
    .select(
        "id",
        "amount",
        "txn_time",
        "merchant_upper",
        "merchant_title",
        "dedup_rank",
        F.col("txn_time").cast("date").alias("txn_time_day"),
        F.coalesce("merchant", F.lit("NONE")).alias("merchant_filled"),
    )
)

summary = (
    valid_transactions
    .groupBy(F.date_trunc("QUARTER", "txn_time").alias("quarter_start"))
    .agg(
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
        F.max("amount").alias("max_amount"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("duration_min"),
        F.stddev_samp("amount").alias("amount_stddev"),
    )
    .filter(F.col("amount_stddev") < 0)
)

result = summary.select("quarter_start", "amount_quartiles", "max_amount", "amount_stddev")
