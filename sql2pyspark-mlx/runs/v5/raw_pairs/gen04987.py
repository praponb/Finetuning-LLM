from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("order_date").desc())

result = (
    df_orders
    .select(
        "id",
        "customer_id",
        "order_date",
        "channel",
        F.dense_rank().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") <= 5)
    .select("id", "customer_id", "order_date", "channel")
    .orderBy("channel")
)
