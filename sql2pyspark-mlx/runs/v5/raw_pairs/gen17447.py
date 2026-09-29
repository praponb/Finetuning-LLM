from pyspark.sql import functions as F

kept_keys = (
    df_transactions
    .select("txn_type", "id")
    .exceptAll(
        df_transactions
        .filter(F.col("txn_type") == "CREDIT")
        .select("txn_type", "id")
    )
)

verified = (
    df_transactions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.txn_type") == F.col("k.txn_type")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy("txn_type")
    .agg(
        F.stddev_samp("amount").alias("amount_stddev"),
        F.count(F.when(F.col("merchant") == "7-Eleven", 1)).alias("7_eleven_count"),
        F.stddev_samp("amount").alias("amount_stddev_2"),
        F.max("txn_time").alias("last_txn_time"),
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("span_minutes"),
    )
    .orderBy(F.col("amount_stddev_2").asc_nulls_first())
)
