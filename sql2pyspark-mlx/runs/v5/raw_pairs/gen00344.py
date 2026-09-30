from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.partitionBy("order_id").orderBy(F.col("amount").desc(), "id")

result = (
    df_payments
    .select(
        "id",
        "status",
        "amount",
        "order_id",
        F.round((F.col("amount") / F.sum("amount").over(w)), 3).alias("amount_share"),
        F.ntile(4).over(w2).alias("amount_ntile"),
    )
    .orderBy(F.col("amount_ntile").desc())
)
