from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("delivered_date", "id")
w2 = Window.partitionBy("status")

result = (
    df_shipments
    .filter(F.col("cost") > 53)
    .select(
        "id",
        "status",
        "delivered_date",
        "cost",
        F.row_number().over(w).alias("rn"),
        F.avg("cost").over(w2).alias("status_avg_cost"),
    )
    .orderBy("status_avg_cost")
)
