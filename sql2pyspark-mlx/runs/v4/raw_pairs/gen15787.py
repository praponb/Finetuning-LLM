from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("status_length").desc_nulls_last(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())

filtered = (
    df_accounts
    .filter(F.col("balance") < 43000)
    .select(
        "id",
        "status",
        F.length("status").alias("status_length"),
        F.coalesce("status", F.lit("UNKNOWN")).alias("resolved_status"),
    )
)

ranked = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

with_last = (
    ranked
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.merchant"))
)

result = (
    with_last
    .withColumn("status_length_tier", F.when(F.col("status_length") >= 68, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["status", "status_length_tier"], ["status"]],
        "status",
        "status_length_tier",
    )
    .agg(
        F.sum(F.when(F.col("status") == "FROZEN", F.col("status_length")).otherwise(0)).alias("frozen_status_length"),
        F.count(F.when(F.col("status") == "CLOSED", 1)).alias("closed_count"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
        F.percentile_approx("status_length", [0.25, 0.5, 0.75]).alias("status_length_quartiles"),
        F.grouping("status").alias("gid"),
    )
    .filter(F.col("closed_count") >= 1)
    .orderBy(F.col("status").desc())
)
