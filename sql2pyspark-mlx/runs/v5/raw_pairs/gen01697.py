from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("method", "status").orderBy("paid_at", "id").rowsBetween(-3, Window.currentRow)
w_running = Window.orderBy(F.col("amount").asc_nulls_last())

scored = (
    df_payments
    .filter(~F.col("status").isin(["REFUNDED", "SUCCESS"]))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "status",
        F.min("amount").over(w_grp).alias("last4_min_amount"),
        F.count("amount").over(w_grp).alias("last4_count_amount"),
        F.max("amount").over(w_grp).alias("last4_max_amount"),
        F.percent_rank().over(w_running).alias("amount_percent_rank"),
    )
)

result = (
    scored
    .select(
        "id",
        "method",
        "status",
        "last4_min_amount",
        "last4_count_amount",
        "last4_max_amount",
        F.when(F.col("status").rlike("^(SUCCESS|FAILED)$"), "CORE").otherwise("PRIORITY").alias("status_zone"),
    )
    .orderBy(F.col("method").asc_nulls_first())
)
