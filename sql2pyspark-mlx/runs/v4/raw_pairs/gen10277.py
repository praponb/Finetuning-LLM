from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("avg_reading").desc())

long_format = (
    df_device_readings
    .select("id", "device_code", "humidity", "pressure", "temp_c")
    .unpivot(
        ["id", "device_code"],
        ["humidity", "pressure", "temp_c"],
        "metric",
        "reading",
    )
    .filter(F.col("reading").isNotNull())
)

result = (
    long_format
    .groupBy("device_code", "metric")
    .agg(
        F.percentile_approx("reading", [0.25, 0.5, 0.75]).alias("reading_quartiles"),
        F.max(F.when(F.col("device_code") == "DV-02", F.col("reading"))).alias("dv_02_max_reading"),
        F.round(F.avg("reading"), 2).alias("avg_reading"),
    )
    .withColumn("avg_reading_rank", F.rank().over(w))
    .orderBy(F.col("device_code").desc_nulls_first())
)
