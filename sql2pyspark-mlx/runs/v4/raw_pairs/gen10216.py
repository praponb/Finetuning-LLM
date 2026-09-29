from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "status").orderBy("delivered_date", "id")

result = (
    df_shipments
    .select(
        "id",
        "order_id",
        "status",
        "delivered_date",
        F.ntile(2).over(w).alias("quartile"),
    )
)
