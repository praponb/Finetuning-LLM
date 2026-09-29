from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w2 = Window.partitionBy("status")

result = (
    df_payments
    .filter(F.col("order_id").isNull())
    .select(
        "id",
        "order_id",
        "status",
        "amount",
        F.count("*").over(w).alias("order_id_count"),
        F.round((F.col("amount") / F.sum("amount").over(w2)), 3).alias("amount_share"),
    )
    .orderBy("amount")
)
