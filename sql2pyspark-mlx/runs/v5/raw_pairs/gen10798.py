from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "device").orderBy("started_at")

result = (
    df_sessions
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        F.dense_rank().over(w).alias("started_at_dense_rank"),
    )
)
