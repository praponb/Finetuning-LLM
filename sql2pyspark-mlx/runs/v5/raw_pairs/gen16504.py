from pyspark.sql import functions as F

long_format = (
    df_device_readings
    .select(
        "id",
        "device_code",
        F.col("humidity").alias("HUMIDITY_VAL"),
        "temp_c",
        F.col("pressure").alias("PRESSURE_VAL"),
    )
    .unpivot(
        ids=["id", "device_code"],
        values=["HUMIDITY_VAL", "temp_c", "PRESSURE_VAL"],
        variableColumnName="kpi",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)

result = (
    long_format
    .groupBy("kpi")
    .agg(
        F.max("value").alias("max_value"),
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("device_code") == "DV-04", 1)).alias("dv_04_rows"),
        F.max("value").alias("max_value_2"),
        F.countDistinct("kpi").alias("distinct_kpis"),
        F.sort_array(F.collect_set("kpi")).alias("kpi_list"),
    )
    .orderBy("cnt")
)
