from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w2 = Window.partitionBy("order_id").orderBy(F.col("amount").desc(), "id")
w_hist = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_payments
    .select(
        "*",
        F.round((F.col("amount") / F.sum("amount").over(w)), 4).alias("amount_share"),
        F.ntile(10).over(w2).alias("amount_bucket"),
        F.first("method", ignorenulls=True).over(w_hist).alias("first_known_method"),
    )
)

result = (
    with_windows
    .select("id", "status", "amount_bucket")
    .orderBy(F.col("id").desc())
)
