from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .filter(F.col("s.status").isin(["IN_TRANSIT", "RETURNED", "DELIVERED"]))
    .groupBy(
        F.col("o.channel"),
        F.when(F.year(F.col("o.order_date")) < 2024, "before_2024").when(F.year(F.col("o.order_date")) == 2024, "2024").otherwise("after_2024").alias("order_cohort"),
    )
    .agg(
        F.max(F.col("s.cost")).alias("max_cost"),
        F.max(F.col("s.shipped_date")).alias("last_shipped_date"),
    )
    .orderBy("last_shipped_date", "channel", "order_cohort")
    .limit(20)
)
