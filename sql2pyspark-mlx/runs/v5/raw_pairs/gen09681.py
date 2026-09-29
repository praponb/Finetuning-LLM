from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("cost").desc())
w2 = Window.partitionBy("order_id").orderBy("shipped_date")

result = (
    df_shipments
    .select(
        "id",
        "order_id",
        "cost",
        "shipped_date",
        F.dense_rank().over(w).alias("cost_dense_rank"),
        F.rank().over(w2).alias("shipped_date_rank"),
    )
)
