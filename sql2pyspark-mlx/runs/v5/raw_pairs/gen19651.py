from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("cost", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("status").orderBy(F.col("shipped_date").desc(), "id")

result = (
    df_shipments
    .select(
        "id",
        "carrier",
        "cost",
        "status",
        F.avg("cost").over(w).alias("cost_avg_6_prior"),
        F.row_number().over(w2).alias("event_seq"),
    )
)
