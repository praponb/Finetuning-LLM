from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id", "method").orderBy(F.col("amount").desc(), "id")

scored = (
    df_payments
    .filter(F.col("status").isin(["REFUNDED", "SUCCESS"]))
    .withColumn("next_amount", F.lead("amount").over(w_recent))
    .withColumn("prev_amount", F.lag("amount").over(w_recent))
    .withColumn("row_seq", F.row_number().over(w_recent))
    .withColumn("row_seq_2", F.row_number().over(w2))
)

result = (
    scored
    .rollup("status", "order_id")
    .agg(
        F.size(F.collect_set("method")).alias("num_methods"),
        F.avg(F.when(F.col("status") == "SUCCESS", F.col("next_amount"))).alias("success_avg_next_amount"),
        F.count_if(F.col("status") == "FAILED").alias("failed_events"),
        F.countDistinct("id").alias("distinct_ids"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("distinct_ids").desc_nulls_last())
)
