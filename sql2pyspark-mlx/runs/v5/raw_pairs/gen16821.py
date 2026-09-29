from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")

result = (
    df_orders
    .select(
        "id",
        "status",
        "discount",
        F.avg("discount").over(w).alias("status_avg_discount"),
    )
)
