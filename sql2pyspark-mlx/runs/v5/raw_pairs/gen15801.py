from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("method").orderBy("amount", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_payments
    .filter(F.col("method") == "cash")
    .select(
        "id",
        "order_id",
        "method",
        F.avg("amount").over(w_grp).alias("last4_avg_amount"),
        F.sum("amount").over(w_grp).alias("last4_sum_amount"),
        F.max("amount").over(w_grp).alias("last4_max_amount"),
        F.min("amount").over(w_grp).alias("last4_min_amount"),
    )
)
