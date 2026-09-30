from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "channel").orderBy(F.col("order_date").desc(), "id")

latest = (
    df_orders
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    latest
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        F.when(F.col("status").rlike("^(CANCELLED|PENDING)$"), "OTHER_ZONE").when(F.col("status").rlike("^(COMPLETED|SHIPPED)$"), "GROUP_A").otherwise("EXTENDED").alias("status_zone"),
    )
    .orderBy("status_zone")
)
