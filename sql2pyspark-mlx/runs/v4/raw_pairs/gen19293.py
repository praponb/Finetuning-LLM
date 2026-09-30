from pyspark.sql import functions as F

order_totals = (
    df_payments
    .filter(F.to_date("paid_at") < "2023-08-01")
    .groupBy("order_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.countDistinct("amount").alias("unique_statuss"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.discount"),
        F.col("o.store_id"),
        F.col("ot.status_values"),
        F.col("ot.unique_statuss"),
    )
)
