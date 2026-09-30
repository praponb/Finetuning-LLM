from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("start_date")
w2 = Window.partitionBy("customer_id").orderBy(F.col("start_date").desc())

result = (
    df_subscriptions
    .select(
        "id",
        "status",
        "start_date",
        "customer_id",
        F.dense_rank().over(w).alias("dense_rnk"),
        F.dense_rank().over(w2).alias("dense_rnk_2"),
    )
)
