from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("delivered_date", "id")

result = (
    df_shipments
    .filter(F.col("order_id").isin([10, 13, 19]))
    .select(
        "id",
        "order_id",
        "delivered_date",
        "cost",
        F.sum("cost").over(w).alias("running_cost"),
    )
)
