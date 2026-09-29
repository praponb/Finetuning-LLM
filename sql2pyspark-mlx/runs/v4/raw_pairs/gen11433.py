from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("order_id").orderBy("amount", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("method").orderBy("paid_at", "id")
w_part = Window.orderBy("paid_at", "id").rowsBetween(-3, Window.currentRow)
w_hist = Window.partitionBy("method", "status").orderBy(F.col("paid_at").desc(), "id")

metrics = (
    df_payments
    .select(
        "*",
        F.avg("amount").over(w_running).alias("amount_avg_3"),
        F.lead("amount", 1, 0).over(w2).alias("next_amount"),
        F.avg("amount").over(w_part).alias("amount_avg_3_2"),
        F.row_number().over(w_hist).alias("seq_no"),
    )
)

result = (
    metrics
    .groupBy(
        F.date_trunc("year", "paid_at").alias("year_start"),
        F.date_trunc("quarter", "paid_at").alias("quarter_start"),
        "method",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.sum("next_amount") / F.nullif(F.count(F.when(F.col("status") == "SUCCESS", 1)), F.lit(0)), 2).alias("next_amount_per_success"),
        F.avg("amount_avg_3_2").alias("avg_amount_avg_3_2"),
    )
    .filter(F.col("next_amount_per_success") >= 0)
)
