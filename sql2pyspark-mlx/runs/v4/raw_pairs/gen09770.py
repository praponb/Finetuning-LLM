from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy(F.col("started_at").desc())
w2 = Window.partitionBy("customer_id", "device").orderBy("started_at")

result = (
    df_sessions
    .select(
        "id",
        "browser",
        "started_at",
        "customer_id",
        "device",
        F.dense_rank().over(w).alias("started_at_dense_rank"),
        F.dense_rank().over(w2).alias("dense_rnk"),
    )
)
