from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)

result = (
    df_payments
    .filter(F.col("paid_at") >= "2023-03-18")
    .select(
        "*",
        F.count("amount").over(w_running).alias("running_count_amount"),
        F.sum("amount").over(w_running).alias("running_sum_amount"),
        F.max("amount").over(w_running).alias("running_max_amount"),
        F.avg("amount").over(w2).alias("amount_avg_30d"),
    )
)
