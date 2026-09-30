from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy(F.col("amount").desc(), "id")

result = (
    df_orders
    .filter(F.month("order_date").isin([5, 7, 8]))
    .select(
        "id",
        "customer_id",
        "order_date",
        "amount",
        "status",
        F.sum("amount").over(w).alias("running_amount"),
        F.row_number().over(w2).alias("row_num"),
    )
)
