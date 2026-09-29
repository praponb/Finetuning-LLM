from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("order_date")

result = (
    df_orders
    .filter(F.col("channel").isin(["web", "mobile", "partner"]))
    .select(
        "id",
        "customer_id",
        "order_date",
        "store_id",
        F.rank().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 3)
    .orderBy(F.col("store_id").desc(), "order_date")
)
