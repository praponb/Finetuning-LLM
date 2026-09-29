from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isin([9, 29, 32, 38]))
    .groupBy("status")
    .agg(
        F.countDistinct("cost").alias("unique_statuss"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
    .filter(F.col("unique_statuss") < 1)
)
