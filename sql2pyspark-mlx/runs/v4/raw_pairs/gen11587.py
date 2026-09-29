from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.orderBy("paid_at", "id")
w_hist = Window.partitionBy("order_id").orderBy("amount")
w3 = Window.orderBy(F.col("paid_at").desc(), "id")
w4 = Window.partitionBy("order_id").orderBy("paid_at", "id")

scored = (
    df_payments
    .withColumn("next_amount", F.lead("amount", 2).over(w_part))
    .withColumn("amount_dense_rank", F.dense_rank().over(w_hist))
    .withColumn("seq_no", F.row_number().over(w3))
    .withColumn("next_amount_2", F.lead("amount", 1, 0).over(w4))
)

result = (
    scored
    .groupBy("seq_no")
    .agg(
        F.stddev_samp("amount").alias("amount_stddev"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.size(F.collect_set("method")).alias("num_methods"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("method") == "promptpay", 1)), F.lit(0)), 2).alias("amount_per_promptpay"),
    )
)
