from pyspark.sql import functions as F

unpivoted = (
    df_device_readings
    .select("id", "device_code", "humidity", "temp_c", "pressure")
    .unpivot(
        ids=["id", "device_code"],
        values=["humidity", "temp_c", "pressure"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)

result = (
    unpivoted
    .groupBy("device_code", "measure")
    .agg(
        F.sum("metric_value").alias("total_metric_value"),
        F.round(F.sum("metric_value") / F.nullif(F.count(F.when(F.col("device_code") == "DV-03", 1)), F.lit(0)), 2).alias("metric_value_per_dv_03"),
        F.count(F.when(F.col("device_code") == "DV-03", 1)).alias("dv_03_count"),
    )
    .filter(F.col("total_metric_value") < 1000000)
)
