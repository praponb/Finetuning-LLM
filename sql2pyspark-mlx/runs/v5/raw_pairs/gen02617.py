from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date") < "2024-03-15")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.min("shipped_date").alias("first_shipped_date"),
    )
    .orderBy("status_values")
)
