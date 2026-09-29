from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("shipping_fee", "id")

result = (
    df_orders
    .select(
        "id",
        "customer_id",
        "shipping_fee",
        "channel",
        F.row_number().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") <= 5)
    .select("id", "customer_id", "shipping_fee", "channel")
    .orderBy("channel")
)
