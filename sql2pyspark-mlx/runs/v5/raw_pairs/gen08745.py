from pyspark.sql import functions as F

filtered = (
    df_transactions
    .filter(F.hour("txn_time").between(6, 9))
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged",
        F.upper("txn_type").alias("txn_type_upper"),
    )
)

result = (
    filtered
    .withColumn("year_start", F.date_trunc("YEAR", "txn_time"))
    .cube("year_start")
    .agg(
        F.max("txn_time").alias("ended_at"),
        F.count_if(F.col("merchant") == "Grab").alias("grab_events"),
        F.size(F.collect_set("txn_type")).alias("num_txn_types"),
        F.max("txn_time").alias("ended_at_2"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("duration_min"),
        F.size(F.collect_set("merchant")).alias("num_merchants"),
        F.grouping("year_start").alias("grouping_level"),
    )
    .filter(F.col("num_merchants") > 1)
    .orderBy(F.col("grab_events").asc_nulls_last())
)
