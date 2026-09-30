from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("delivered_date").desc(), "id")

result = (
    df_shipments
    .filter(F.col("status") != "DELIVERED")
    .select(
        "id",
        "order_id",
        "delivered_date",
        "cost",
        F.row_number().over(w).alias("rn"),
    )
    .filter(F.col("rn") <= 5)
    .select("id", "order_id", "delivered_date", "cost")
    .orderBy("order_id")
)
