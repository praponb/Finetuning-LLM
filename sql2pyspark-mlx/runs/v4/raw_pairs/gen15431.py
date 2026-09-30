from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("cost").desc())
w2 = Window.partitionBy("order_id").orderBy("shipped_date", "id")

result = (
    df_shipments
    .filter(F.col("delivered_date") <= "2023-07-29")
    .select(
        "id",
        "order_id",
        "cost",
        "shipped_date",
        F.dense_rank().over(w).alias("cost_dense_rank"),
        (F.col("cost") - F.lag("cost", 1).over(w2)).alias("cost_change"),
    )
)
