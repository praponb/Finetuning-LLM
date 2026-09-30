from pyspark.sql import functions as F

result = (
    df_orders
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
)
