from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("order_date", "id")
w2 = Window.partitionBy("status").orderBy("order_date")

result = (
    df_orders
    .filter(F.year("order_date") >= 2024)
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        F.row_number().over(w).alias("row_num"),
        F.rank().over(w2).alias("rnk"),
    )
    .orderBy(F.col("customer_id").desc())
)
