from pyspark.sql import functions as F

recent_orders = (
    df_orders
    .filter((F.col("amount") < 1060) & (F.col("order_date") > "2025-07-04"))
    .withColumn("discount_adjusted", F.round((F.col("discount") * 100), 2))
)

summary = (
    recent_orders
    .groupBy(F.quarter("order_date").alias("order_quarter"))
    .agg(F.sort_array(F.collect_set("status")).alias("status_list"))
)

result = (
    summary
    .select("order_quarter", "status_list")
    .orderBy(F.col("order_quarter").desc())
)
