from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.count("*").alias("total_count"),
    )
    .filter(F.col("total_count") >= 8)
    .select("status", "status_values", "total_count")
    .orderBy(F.col("status").desc())
)
