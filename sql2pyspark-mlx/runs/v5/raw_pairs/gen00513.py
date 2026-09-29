from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status")
w_grp = Window.partitionBy("order_id").orderBy("paid_at", "id")
w4 = Window.orderBy(F.col("amount").desc(), "id")
w5 = Window.partitionBy("method")

result = (
    df_payments
    .filter(~F.col("method").isin(["cash", "paypal", "card"]))
    .select(
        "id",
        "paid_at",
        "status",
        F.sum("amount").over(w).alias("running_amount"),
        F.round((F.col("amount") / F.sum("amount").over(w2)), 4).alias("amount_share"),
        F.row_number().over(w_grp).alias("seq_no"),
        F.ntile(4).over(w4).alias("amount_bucket"),
        F.round((F.col("amount") / F.sum("amount").over(w5)), 4).alias("amount_share_2"),
    )
)
