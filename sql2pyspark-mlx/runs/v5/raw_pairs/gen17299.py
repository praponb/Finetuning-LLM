from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("cost").desc())
w2 = Window.partitionBy("carrier").orderBy("shipped_date", "id")

result = (
    df_shipments
    .filter(F.col("carrier").isin(["Kerry", "Flash"]))
    .select(
        "id",
        "status",
        "cost",
        "carrier",
        "shipped_date",
        F.dense_rank().over(w).alias("dense_rnk"),
        F.lag("cost", 1).over(w2).alias("prev_cost"),
    )
)
