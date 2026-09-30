from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id")

result = (
    df_payments
    .filter(~F.col("method").isin(["bank_transfer", "card"]))
    .select(
        "id",
        "order_id",
        "status",
        "paid_at",
        F.count("*").over(w).alias("order_id_count"),
        F.ntile(2).over(w2).alias("bucket"),
    )
)
