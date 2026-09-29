from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("amount", "id")

result = (
    df_payments
    .filter(F.col("status") == "SUCCESS")
    .select(
        "id",
        "method",
        "paid_at",
        "status",
        F.lead("amount").over(w).alias("next_amount"),
        F.lag("amount").over(w).alias("prev_amount"),
        F.row_number().over(w).alias("row_seq"),
    )
)
