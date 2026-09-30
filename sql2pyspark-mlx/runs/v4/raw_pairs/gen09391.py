from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("started_at").desc(), "id")
w2 = Window.partitionBy("device").orderBy("started_at")

result = (
    df_sessions
    .filter(F.col("customer_id") == 27)
    .select(
        "id",
        "started_at",
        "device",
        F.row_number().over(w).alias("rn"),
        F.dense_rank().over(w2).alias("started_at_dense_rank"),
    )
)
