from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_orders
    .filter(F.col("amount").between(1120, 1170))
    .select(
        "id",
        "status",
        "order_date",
        "shipping_fee",
        F.sum("shipping_fee").over(w).alias("running_shipping_fee"),
    )
)
