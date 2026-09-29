from pyspark.sql import functions as F

base = (
    df_orders
    .filter((F.col("amount") >= 1430) & (F.col("discount") >= 0.14))
    .withColumn("discount_rounded", F.round("discount", 2))
)

agg = (
    base
    .groupBy("channel")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
)

result = (
    agg
    .select("channel", "status_values")
    .orderBy("status_values", "channel")
    .limit(50)
)
