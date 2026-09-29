from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc())
amount_by_customer_id = df_orders.groupBy("customer_id").agg(F.sum("amount").alias("sum_order_amount"))

deduped = (
    df_sessions
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    deduped
    .join(amount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "rank_in_group",
        F.col("sum_order_amount"),
    )
    .orderBy("customer_id")
)
