from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("shipped_date").desc())
w2 = Window.partitionBy("order_id").orderBy("delivered_date", "id")

result = (
    df_shipments
    .filter(F.col("carrier").isin(["UPS", "Kerry"]))
    .select(
        "id",
        "shipped_date",
        "order_id",
        "delivered_date",
        "cost",
        F.dense_rank().over(w).alias("dense_rnk"),
        F.first("cost").over(w2).alias("first_cost"),
    )
    .orderBy("shipped_date")
)
