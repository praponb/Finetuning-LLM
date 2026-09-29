from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("order_date")
discount_by_customer_id = df_orders.groupBy("customer_id").agg(F.min("discount").alias("min_order_discount"))

ranked = (
    df_orders
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .join(discount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "status",
        "amount",
        F.when(F.col("status").rlike("^(SHIPPED|PENDING)$"), "OTHER_ZONE").when(F.col("status").rlike("^(COMPLETED|CANCELLED)$"), "GROUP_A").otherwise("PRIORITY").alias("status_zone"),
        F.col("min_order_discount"),
    )
    .orderBy(F.col("status_zone").desc())
)
