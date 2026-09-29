from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "left")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "left")
    .groupBy(
        F.when(F.year(F.col("s.opened_date")) < 2024, "before_2024").when(F.year(F.col("s.opened_date")) == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("p.status")))).alias("status_values"),
    )
    .orderBy(F.col("opened_cohort").desc())
    .limit(20)
)
