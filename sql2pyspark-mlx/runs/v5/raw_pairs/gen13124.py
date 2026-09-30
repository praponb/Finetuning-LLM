from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id")

result = (
    df_payments
    .filter(F.hour("paid_at").between(11, 17))
    .select(
        "id",
        "paid_at",
        "amount",
        "status",
        F.lead("amount", 1, 0).over(w).alias("next_amount"),
        F.lead("amount", 2).over(w2).alias("next_amount_2"),
    )
)
