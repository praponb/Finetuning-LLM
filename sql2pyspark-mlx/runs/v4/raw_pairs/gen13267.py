from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("paid_at").desc())

result = (
    df_payments
    .filter(F.col("status") == "SUCCESS")
    .select(
        "id",
        "order_id",
        "paid_at",
        F.dense_rank().over(w).alias("paid_at_dense_rank"),
    )
)
