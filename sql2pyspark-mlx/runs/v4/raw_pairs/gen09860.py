from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("delivered_date", "id")
w2 = Window.partitionBy("status")

result = (
    df_shipments
    .filter(F.col("shipped_date") > "2025-10-01")
    .select(
        "id",
        "carrier",
        "delivered_date",
        "shipped_date",
        "status",
        "cost",
        F.first("shipped_date").over(w).alias("first_shipped_date"),
        F.sum("cost").over(w2).alias("status_sum_cost"),
    )
)
