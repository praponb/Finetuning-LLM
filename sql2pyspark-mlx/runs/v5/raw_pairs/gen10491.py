from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc())

result = (
    df_sessions
    .select(
        "id",
        "customer_id",
        "started_at",
        F.dense_rank().over(w).alias("started_at_dense_rank"),
    )
)
