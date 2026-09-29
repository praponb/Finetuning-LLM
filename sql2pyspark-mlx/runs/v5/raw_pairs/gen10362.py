from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("delivered_date")

result = (
    df_shipments
    .filter(F.col("status").isin(["IN_TRANSIT", "LOST"]))
    .select(
        "id",
        "order_id",
        "delivered_date",
        F.dense_rank().over(w).alias("dense_rnk"),
    )
)
