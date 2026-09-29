from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w2 = Window.partitionBy("method").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_payments
    .select(
        "*",
        F.count("amount").over(w).alias("amount_count_30d"),
        F.sum("amount").over(w2).alias("running_amount"),
    )
)
