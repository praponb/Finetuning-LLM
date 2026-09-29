from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .agg(
        F.count("*").alias("cnt"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
    .filter(F.col("cnt") >= 3)
    .orderBy("status_values")
)
