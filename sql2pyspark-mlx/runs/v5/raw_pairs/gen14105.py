from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("amount", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_payments
    .filter(F.col("status") == "SUCCESS")
    .select(
        "id",
        "amount",
        "status",
        F.sum("amount").over(w).alias("last4_sum_amount"),
        F.min("amount").over(w).alias("last4_min_amount"),
        F.count("amount").over(w).alias("last4_count_amount"),
        F.max("amount").over(w).alias("last4_max_amount"),
    )
)
