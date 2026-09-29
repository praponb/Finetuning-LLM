from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("amount").desc_nulls_first())
w2 = Window.orderBy("amount", "id")
w3 = Window.partitionBy("order_id", "status")
w_ordered = Window.partitionBy("method").orderBy("amount", "id").rowsBetween(-6, Window.currentRow)
w5 = Window.orderBy(F.col("amount").desc(), "id")

result = (
    df_payments
    .filter(F.col("status") == "FAILED")
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "status",
        F.rank().over(w).alias("amount_rank"),
        F.lead("amount", 2).over(w2).alias("next_amount"),
        F.round((F.col("amount") / F.sum("amount").over(w3)), 4).alias("amount_share"),
        F.avg("amount").over(w_ordered).alias("amount_avg_6"),
        F.ntile(4).over(w5).alias("amount_bucket"),
    )
)
