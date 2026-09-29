from pyspark.sql import functions as F

customer_totals = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .groupBy("customer_id")
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.sum(F.when(F.col("plan") == "basic", 1).otherwise(0)).alias("basic_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .select(F.col("c.name"), F.col("t.status_list"), F.col("t.basic_count"))
)
