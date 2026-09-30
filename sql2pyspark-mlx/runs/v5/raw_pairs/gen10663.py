from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "carrier").orderBy("delivered_date", "id")
w2 = Window.partitionBy("carrier")

result = (
    df_shipments
    .select(
        "id",
        "status",
        "carrier",
        "delivered_date",
        "cost",
        F.lead("cost", 1).over(w).alias("next_cost"),
        F.round((F.col("cost") / F.sum("cost").over(w2)), 3).alias("cost_share"),
    )
)
