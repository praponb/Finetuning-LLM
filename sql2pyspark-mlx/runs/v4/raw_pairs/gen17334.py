from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("cost")
w2 = Window.partitionBy("carrier").orderBy("delivered_date", "id")

result = (
    df_shipments
    .select(
        "id",
        "carrier",
        "cost",
        "delivered_date",
        F.rank().over(w).alias("rnk"),
        F.sum("cost").over(w2).alias("running_cost"),
    )
)
