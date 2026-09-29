from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method")
w2 = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_payments
    .filter(F.col("order_id").isNotNull() & (F.to_date("paid_at") < "2023-10-01"))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "status",
        F.substring("method", 1, 3).alias("method_prefix"),
    )
)

result = (
    cleaned
    .select(
        "*",
        F.round((F.col("amount") / F.sum("amount").over(w)), 4).alias("amount_share"),
        F.last("amount", ignorenulls=True).over(w2).alias("last_known_amount"),
    )
)
