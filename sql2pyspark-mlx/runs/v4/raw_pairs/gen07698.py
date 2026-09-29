from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "inner")
    .groupBy(
        F.col("orders.status"),
        F.quarter(F.col("orders.order_date")).alias("order_quarter"),
    )
    .agg(
        F.countDistinct(F.col("shipments.cost")).alias("unique_orders"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("shipments.carrier")))).alias("carrier_values"),
    )
    .filter(F.col("unique_orders") > 4)
    .orderBy("unique_orders", "order_quarter")
)
