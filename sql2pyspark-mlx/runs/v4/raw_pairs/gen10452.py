from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("amount").desc(), "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.partitionBy("order_id", "method").orderBy("paid_at", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.partitionBy("order_id").orderBy(F.col("amount").desc())

scored = (
    df_payments
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.avg("amount").over(w2).alias("amount_avg_3"),
        F.avg("amount").over(w3).alias("amount_avg_5"),
        F.avg("amount").over(w4).alias("amount_avg_3_2"),
        F.rank().over(w5).alias("amount_rank"),
    )
)

result = (
    scored
    .groupBy(F.date_trunc("YEAR", "paid_at").alias("year_start"))
    .agg(
        F.count(F.when(F.col("status") == "REFUNDED", 1)).alias("refunded_count"),
        F.size(F.collect_set("status")).alias("num_status"),
    )
    .orderBy(F.col("num_status").desc())
)
