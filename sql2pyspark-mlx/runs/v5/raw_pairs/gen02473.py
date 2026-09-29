from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "store_id").orderBy("order_date", "id")

result = (
    df_orders
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "discount",
        F.sum("discount").over(w).alias("running_discount"),
    )
    .orderBy(F.col("discount").desc())
)
