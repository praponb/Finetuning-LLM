from pyspark.sql import functions as F

exploded = df_app_logs.select("id", "log_ts", F.explode_outer("context").alias("k", "v"))

result = (
    exploded
    .withColumn("hour_window", F.date_trunc("hour", "log_ts"))
    .groupingSets([["k", "hour_window"], ["k"]], "k", "hour_window")
    .agg(
        F.count(F.when(F.col("k") == "version", 1)).alias("version_rows"),
        F.max("log_ts").alias("last_seen"),
        F.sort_array(F.collect_set("k")).alias("k_list"),
        F.countDistinct("k").alias("distinct_ks"),
        F.countDistinct("k").alias("distinct_ks_2"),
    )
    .filter(F.col("version_rows") < 1)
)
