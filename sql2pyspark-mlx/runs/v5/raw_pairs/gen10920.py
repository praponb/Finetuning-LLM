from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("delivered_date", "id")

result = (
    df_shipments
    .filter(F.col("status") == "IN_TRANSIT")
    .select(
        "id",
        "status",
        "delivered_date",
        "cost",
        F.sum("cost").over(w).alias("running_cost"),
    )
)
