from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "inner")
    .filter(F.year(F.col("s.shipped_date")) == 2025)
    .groupBy(F.col("st.city"))
    .agg(
        F.sum(F.when(F.col("s.status") == "RETURNED", F.col("s.cost")).otherwise(0)).alias("returned_cost"),
        F.sort_array(F.collect_set(F.col("s.carrier"))).alias("carrier_list"),
    )
    .orderBy("returned_cost")
)
