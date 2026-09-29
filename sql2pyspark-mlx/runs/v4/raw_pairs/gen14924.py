from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "status").orderBy("amount", "id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("shipping_fee").desc())

result = (
    df_orders
    .filter(F.col("shipping_fee") > 21)
    .select(
        "id",
        "channel",
        "status",
        "amount",
        "customer_id",
        "shipping_fee",
        F.row_number().over(w).alias("rn"),
        F.percent_rank().over(w2).alias("shipping_fee_pct_rank"),
    )
)
