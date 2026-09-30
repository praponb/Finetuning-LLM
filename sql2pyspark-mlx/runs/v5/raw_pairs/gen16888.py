from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("delivered_date", "id")
w2 = Window.partitionBy("status").orderBy(F.col("shipped_date").desc())

result = (
    df_shipments
    .select(
        "id",
        "delivered_date",
        "cost",
        "status",
        "shipped_date",
        F.lead("cost", 1).over(w).alias("next_cost"),
        F.dense_rank().over(w2).alias("shipped_date_dense_rank"),
    )
)
