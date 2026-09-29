from pyspark.sql import functions as F

not_flagged = (
    df_orders
    .alias("s")
    .join(df_payments.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.paid_at") >= F.col("s.order_date")), "left_anti")
)

result = (
    not_flagged
    .groupBy("store_id")
    .agg(
        F.count(F.when(F.col("status") == "CANCELLED", 1)).alias("cancelled_rows"),
        F.size(F.collect_set("channel")).alias("num_channels"),
    )
)
