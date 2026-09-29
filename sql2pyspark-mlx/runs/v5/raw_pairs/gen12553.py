from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "order_id").orderBy("shipped_date", "id")
w2 = Window.partitionBy("order_id")

result = (
    df_shipments
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "cost",
        "status",
        F.lead("cost").over(w).alias("next_cost"),
        F.lag("cost").over(w).alias("prev_cost"),
        F.row_number().over(w).alias("row_seq"),
        F.round((F.col("cost") / F.sum("cost").over(w2)), 4).alias("cost_share"),
    )
)
