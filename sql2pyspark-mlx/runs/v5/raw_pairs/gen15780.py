from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("delivered_date", "id")
w_grp = Window.partitionBy("order_id", "status").orderBy("shipped_date", "id")

result = (
    df_shipments
    .filter(F.col("shipped_date") <= "2025-02-01")
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        F.lead("cost", 1).over(w).alias("next_cost"),
        F.lag("cost", 1).over(w_grp).alias("prev_cost"),
    )
)
