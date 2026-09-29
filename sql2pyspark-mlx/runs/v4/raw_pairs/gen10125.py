from pyspark.sql import functions as F

metrics_long = (
    df_device_readings
    .select("id", "device_code", "humidity", "pressure", "temp_c")
    .unpivot(
        ["id", "device_code"],
        ["humidity", "pressure", "temp_c"],
        "measure",
        "metric_value",
    )
)

result = (
    metrics_long
    .groupBy(
        F.when(F.col("metric_value") >= 86, "LARGE").otherwise("SMALL").alias("metric_value_tier"),
    )
    .agg(
        F.max(F.when(F.col("measure") == "temp_c", F.col("metric_value"))).alias("temp_c_max_metric_value"),
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
        F.percentile_approx("metric_value", 0.95).alias("metric_value_p95"),
        F.max(F.when(F.col("device_code") == "DV-03", F.col("metric_value"))).alias("dv_03_max_metric_value"),
    )
    .orderBy(F.col("temp_c_max_metric_value").asc_nulls_last())
)
