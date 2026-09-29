from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_transactions
    .withColumn("new_group_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

islands = with_gaps.withColumn("island_id", F.sum("new_group_flag").over(w2))

result = (
    islands
    .groupBy(
        F.when(F.col("amount") >= 3300, "LARGE").otherwise("SMALL").alias("amount_tier"),
    )
    .agg(
        F.avg(F.when(F.col("txn_type") == "FEE", F.col("island_id"))).alias("fee_avg_island_id"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("span_minutes"),
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
        F.max(F.when(F.col("merchant") == "Lazada", F.col("amount"))).alias("lazada_max_amount"),
    )
    .filter(F.col("lazada_max_amount") > 0)
    .orderBy(F.col("span_minutes").asc_nulls_last())
)
