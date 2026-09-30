from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("discount").desc(), "id")
w_part = Window.partitionBy("status")

result = (
    df_orders
    .select(
        "id",
        "order_date",
        "status",
        "channel",
        "discount",
        "shipping_fee",
        F.row_number().over(w).alias("seq_no"),
        F.corr("amount", "discount").over(w_part).alias("corr_amount_discount"),
    )
)
