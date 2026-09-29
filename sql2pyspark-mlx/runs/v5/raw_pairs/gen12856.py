from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.concat_ws(", ", F.sort_array(F.collect_set("carrier"))).alias("carrier_values"),
    )
)
