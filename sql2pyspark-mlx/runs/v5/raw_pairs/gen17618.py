from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method", "order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_payments
    .filter(F.col("paid_at") >= "2025-02-06")
    .select(
        "id",
        "order_id",
        F.max("amount").over(w).alias("running_max_amount"),
        F.sum("amount").over(w).alias("running_sum_amount"),
        F.count("amount").over(w).alias("running_count_amount"),
    )
)

result = (
    enriched
    .select("id", "running_max_amount", "running_sum_amount", "running_count_amount")
)
