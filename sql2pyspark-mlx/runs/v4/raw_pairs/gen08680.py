from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_shipments
    .select(
        "id",
        "status",
        "shipped_date",
        "delivered_date",
        F.last("delivered_date").over(w).alias("last_delivered_date"),
    )
)
