from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "method")
w2 = Window.partitionBy("order_id", "status").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w3 = Window.orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status", "method").orderBy("paid_at_day", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("method")

base = (
    df_payments
    .filter((F.col("paid_at") > F.to_date(F.lit("2023-07-24"))) & F.col("order_id").isNotNull())
    .select("*", F.col("paid_at").cast("date").alias("paid_at_day"))
)

result = (
    base
    .select(
        "*",
        F.sum("amount").over(w).alias("status_sum_amount"),
        F.max("amount").over(w2).alias("amount_max_90d"),
        F.first("method", ignorenulls=True).over(w3).alias("first_known_method"),
        F.last("amount", ignorenulls=True).over(w4).alias("last_known_amount"),
        F.sum("amount").over(w_running).alias("method_sum_amount"),
    )
)
