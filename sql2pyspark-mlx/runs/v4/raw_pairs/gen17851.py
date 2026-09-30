from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount") > 0.29)
    .groupBy("status")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("channel"))).alias("channel_values"),
        F.countDistinct("discount").alias("unique_stores"),
    )
    .filter(F.col("unique_stores") >= 4)
    .orderBy(F.col("channel_values").desc(), F.col("unique_stores").desc(), "status")
    .limit(10)
)
