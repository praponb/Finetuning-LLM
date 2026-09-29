from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("paid_at").desc(), "id")
w3 = Window.partitionBy("order_id").orderBy("amount", "id")
w4 = Window.partitionBy("order_id").orderBy("paid_at", "id")

scored = (
    df_payments
    .select(
        "id",
        "method",
        "paid_at",
        "status",
        F.last("method", ignorenulls=True).over(w).alias("last_known_method"),
        F.row_number().over(w2).alias("seq_no"),
        F.ntile(3).over(w3).alias("amount_bucket"),
        F.lead("amount", 1).over(w4).alias("next_amount"),
    )
)

result = (
    scored
    .select(
        "id",
        "method",
        "paid_at",
        "last_known_method",
        "seq_no",
        F.when(F.col("method").rlike("^(cash|paypal)$"), "OTHER_ZONE").when(F.col("method").rlike("^(card|bank_transfer)$"), "CORE").otherwise("GROUP_A").alias("method_zone"),
    )
)
