from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("txn_time").desc_nulls_first(), "id")

deduped = (
    df_transactions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .groupBy(
        F.when(F.col("amount") >= 3700, "LARGE").otherwise("SMALL").alias("amount_tier"),
        "merchant",
    )
    .agg(
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
        F.count(F.when(F.col("merchant") == "7-Eleven", 1)).alias("7_eleven_count"),
        F.countDistinct("account_id").alias("distinct_account_ids"),
        F.count(F.when(F.col("merchant") == "Shell", 1)).alias("shell_rows"),
        F.percentile_approx("amount", 0.5).alias("amount_p50"),
        F.size(F.collect_set("txn_type")).alias("num_txn_types"),
    )
    .filter(F.col("amount_p50") < 0)
    .orderBy(F.col("distinct_account_ids").asc_nulls_first())
)
