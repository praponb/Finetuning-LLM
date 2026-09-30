from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount").between(1060, 1280) & (F.col("order_date") <= "2023-10-01"))
    .groupBy("channel")
    .agg(
        F.count(F.when(F.col("status") == "CANCELLED", 1)).alias("cancelled_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
    .filter(F.col("cancelled_count") > 5)
)
