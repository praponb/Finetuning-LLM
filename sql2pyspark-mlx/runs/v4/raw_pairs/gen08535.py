from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w2 = Window.partitionBy("method", "order_id").orderBy(F.col("paid_at").desc(), "id")
w_recent = Window.partitionBy("status").orderBy(F.col("amount").desc())
w_ordered = Window.partitionBy("order_id").orderBy("amount", "id")
w5 = Window.partitionBy("status", "method").orderBy("amount", "id")

scored = (
    df_payments
    .select(
        "*",
        F.avg("amount").over(w).alias("order_id_avg_amount"),
        F.row_number().over(w2).alias("event_seq"),
        F.dense_rank().over(w_recent).alias("amount_dense_rank"),
        F.lag("amount", 1).over(w_ordered).alias("prev_amount"),
        F.lag("amount", 1).over(w5).alias("prev_amount_2"),
    )
)

result = (
    scored
    .cube(
        F.when(F.col("prev_amount") >= 1650, "HIGH").otherwise("LOW").alias("prev_amount_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("method")).alias("method_list"),
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("duration_min"),
        F.percentile_approx("prev_amount", [0.25, 0.5, 0.75]).alias("prev_amount_quartiles"),
        F.count(F.when(F.col("method") == "paypal", 1)).alias("paypal_rows"),
    )
)
