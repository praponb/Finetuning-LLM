from pyspark.sql import functions as F

base = (
    df_payments
    .filter(F.col("order_id").isNotNull() & (~F.col("method").eqNullSafe(F.lit("card"))))
    .withColumn("method_status", F.concat("method", F.lit(", "), "status"))
    .withColumn("resolved_method", F.coalesce("method", F.lit("NONE")))
)

result = (
    base
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id") == F.col("p.id"), "left")
    .groupingSets(
        [["channel", "payment_status"], ["channel"]],
        F.col("p.channel").alias("channel"),
        F.col("a.status").alias("payment_status"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.avg(F.col("a.amount")).alias("avg_amount"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("rows_total").asc_nulls_first())
)
