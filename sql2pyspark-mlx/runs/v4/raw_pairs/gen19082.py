from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "channel").orderBy(F.col("discount").desc(), "id")

deduped = (
    df_orders
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .select(
        "id",
        "order_date",
        "status",
        "channel",
        "discount",
        "shipping_fee",
        "latest_rank",
    )
    .orderBy("latest_rank", "order_date")
)
