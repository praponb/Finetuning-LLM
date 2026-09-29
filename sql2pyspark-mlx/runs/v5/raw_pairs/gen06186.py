from pyspark.sql import functions as F

cleaned = (
    df_orders
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "amount",
        "discount",
        F.when(F.col("channel") == "web", 1).otherwise(0).alias("is_web"),
        F.length("status").alias("status_length"),
    )
)

result = (
    cleaned
    .groupBy("customer_id")
    .pivot("status", ["PENDING", "COMPLETED"])
    .agg(F.max("amount").alias("value_sum"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "customer_id",
        F.col("PENDING_value_sum").alias("pending_value_sum"),
        F.col("PENDING_cnt").alias("pending_cnt"),
        F.col("COMPLETED_value_sum").alias("completed_value_sum"),
        F.col("COMPLETED_cnt").alias("completed_cnt"),
    )
)
