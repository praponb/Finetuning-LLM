from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("days_since_order_date").asc_nulls_last())

cleaned = (
    df_orders
    .filter((F.col("order_date") > F.lit("2023-09-19").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "order_date",
        "status",
        "amount",
        "shipping_fee",
        F.datediff(F.current_date(), "order_date").alias("days_since_order_date"),
        F.concat("status", F.lit(" "), "channel").alias("status_channel"),
        F.coalesce("channel", F.lit("NONE")).alias("channel_clean"),
    )
)

result = (
    cleaned
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)
