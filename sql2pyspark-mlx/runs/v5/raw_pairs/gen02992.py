from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("order_date", "id")

result = (
    df_orders
    .filter(~F.col("channel").isin(["web", "mobile"]))
    .select(
        "id",
        "status",
        "order_date",
        "amount",
        F.lead("amount", 1).over(w).alias("next_amount"),
    )
    .orderBy("status", "amount")
)
