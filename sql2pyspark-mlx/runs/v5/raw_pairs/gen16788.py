from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status", "method")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
    .orderBy("method")
)
