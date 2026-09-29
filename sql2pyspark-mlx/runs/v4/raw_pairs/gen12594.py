from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type", "account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("account_id").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w_part = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part_2 = Window.partitionBy("account_id").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w5 = Window.partitionBy("txn_type").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w6 = Window.partitionBy("merchant", "txn_type").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w_grp = Window.partitionBy("txn_type").orderBy("amount", "id")
w8 = Window.partitionBy("account_id", "txn_type").orderBy(F.col("amount_avg_7d").desc_nulls_first(), "id")

scored = (
    df_transactions
    .select(
        "*",
        F.last("amount", ignorenulls=True).over(w).alias("last_known_amount"),
        F.avg("amount").over(w_recent).alias("amount_avg_7d"),
    )
)

scored_transactions = (
    scored
    .withColumn("first_known_amount", F.first("amount", ignorenulls=True).over(w_part))
    .withColumn("amount_sum_30d", F.sum("amount").over(w_part_2))
)

enriched = (
    scored_transactions
    .withColumn("last_known_amount_avg_90d", F.avg("last_known_amount").over(w5))
    .withColumn("amount_avg_7d_count_7d", F.count("amount_avg_7d").over(w6))
    .withColumn("row_seq", F.row_number().over(w_grp))
)

ranked = (
    enriched
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    ranked
    .groupBy("merchant")
    .pivot("txn_type", ["REFUND", "FEE"])
    .agg(F.avg("row_seq").alias("total"), F.count(F.lit(1)).alias("n"))
)
