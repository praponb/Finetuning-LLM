from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("delivered_date", "id")

result = (
    df_shipments
    .filter(F.col("status") != "RETURNED")
    .select(
        "id",
        "delivered_date",
        "status",
        F.first("status").over(w).alias("first_status"),
    )
)
