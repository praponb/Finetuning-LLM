from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method")

result = (
    df_payments
    .filter(F.hour("paid_at").between(11, 18))
    .select(
        "id",
        "method",
        "amount",
        F.round((F.col("amount") / F.sum("amount").over(w)), 3).alias("amount_share"),
    )
)
