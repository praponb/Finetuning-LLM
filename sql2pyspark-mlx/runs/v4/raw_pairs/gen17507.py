from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("order_date_period", "id")
w2 = Window.partitionBy("channel").orderBy("order_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy("discount")
w4 = Window.partitionBy("customer_id").orderBy(F.col("order_date_period").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w5 = Window.partitionBy("channel").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("status").orderBy(F.col("discount").desc_nulls_last(), "id")
shipping_fee_by_store_id = df_orders.groupBy("store_id").agg(F.min("shipping_fee").alias("min_order_shipping_fee"))

valid = (
    df_orders
    .filter(F.col("customer_id").isNotNull())
    .select("*", F.date_trunc("YEAR", "order_date").alias("order_date_period"))
)

flagged = (
    valid
    .withColumn("is_new_session", F.when(F.lag("order_date_period").over(w).isNull() | (F.col("order_date_period") > F.lag("order_date_period").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_orders = flagged.withColumn("burst_id", F.sum("is_new_session").over(w2))

scored = (
    sessions_orders
    .withColumn("discount_percent_rank", F.percent_rank().over(w3))
    .withColumn("is_new_session_count_90d", F.count("is_new_session").over(w4))
    .withColumn("first_known_amount", F.first("amount", ignorenulls=True).over(w5))
)

ranked = (
    scored
    .withColumn("dedup_rank", F.row_number().over(w6))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked
    .join(shipping_fee_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "order_date",
        "channel",
        "amount",
        "order_date_period",
        "is_new_session",
        "burst_id",
        "discount_percent_rank",
        F.col("min_order_shipping_fee"),
    )
    .orderBy(F.col("order_date").desc_nulls_first())
)
