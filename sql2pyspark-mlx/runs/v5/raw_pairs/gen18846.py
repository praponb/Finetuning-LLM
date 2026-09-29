from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("discount").desc(), "id")
w2 = Window.partitionBy("store_id").orderBy("order_date", "id").rowsBetween(-5, -1)

result = (
    df_orders
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        F.row_number().over(w).alias("event_seq"),
        F.avg("discount").over(w2).alias("discount_avg_5_prior"),
    )
)
