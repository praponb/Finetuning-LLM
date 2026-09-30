from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("discount").desc())

result = (
    df_orders
    .select(
        "id",
        "channel",
        "discount",
        F.dense_rank().over(w).alias("discount_dense_rank"),
    )
)
