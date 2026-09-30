from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("week_start").orderBy(F.col("amount_per_7_eleven").desc())

kept_keys = (
    df_transactions
    .select("account_id", "merchant")
    .subtract(
        df_transactions
        .filter(F.col("txn_type") != "CREDIT")
        .select("account_id", "merchant")
    )
)

reconciled = (
    df_transactions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.account_id") == F.col("k.account_id")) & (F.col("s.merchant") == F.col("k.merchant")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.date_trunc("WEEK", "txn_time").alias("week_start"),
        F.date_trunc("YEAR", "txn_time").alias("year_start"),
        F.when(F.col("amount") >= 4200, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
    )
    .agg(
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("duration_min"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("merchant") == "7-Eleven", 1)), F.lit(0)), 2).alias("amount_per_7_eleven"),
    )
    .filter(F.col("amount_per_7_eleven") >= 0)
    .withColumn("amount_per_7_eleven_percent_rank", F.percent_rank().over(w))
)
