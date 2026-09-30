from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter(F.col("s.cost").between(57, 95))
    .groupBy(F.col("o.status"))
    .agg(
        F.sum(F.when(F.col("s.status") == "RETURNED", 1).otherwise(0)).alias("returned_count"),
        F.sort_array(F.collect_set(F.col("s.carrier"))).alias("carrier_list"),
    )
    .orderBy("returned_count")
)
