from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("method").orderBy(F.col("amount").desc(), "id")

filtered = (
    df_payments
    .filter(F.col("order_id").isin([10, 28, 39]))
    .select(
        "*",
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
        F.date_format("paid_at", "MMM yyyy").alias("paid_at_label"),
    )
)

result = (
    filtered
    .select(
        "*",
        F.last("amount", ignorenulls=True).over(w).alias("last_known_amount"),
        F.row_number().over(w2).alias("row_seq"),
    )
)
