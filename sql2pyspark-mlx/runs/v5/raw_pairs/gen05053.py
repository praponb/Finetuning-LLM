from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(-6, -1)
w_grp = Window.partitionBy("customer_id", "channel").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("channel").orderBy(F.col("discount").desc(), "id")

result = (
    df_orders
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "amount",
        "shipping_fee",
        F.avg("discount").over(w).alias("discount_avg_6_prior"),
        F.sum("discount").over(w_grp).alias("running_discount"),
        F.row_number().over(w3).alias("seq_no"),
    )
)
