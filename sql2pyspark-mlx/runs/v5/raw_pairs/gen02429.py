from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("measure").orderBy(F.col("val_p50").desc())

metrics_long = (
    df_device_readings
    .select("id", "device_code", "temp_c", "pressure", "humidity")
    .unpivot(
        ids=["id", "device_code"],
        values=["temp_c", "pressure", "humidity"],
        variableColumnName="measure",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)

result = (
    metrics_long
    .groupBy(
        "measure",
        F.when(F.col("val") >= 86, "LARGE").otherwise("SMALL").alias("val_tier"),
    )
    .agg(
        F.round(F.avg("val"), 2).alias("avg_val"),
        F.percentile_approx("val", 0.5).alias("val_p50"),
        F.sum(F.when(F.col("device_code") == "DV-04", F.col("val")).otherwise(0)).alias("dv_04_val"),
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
        F.count_if(F.col("measure") == "temp_c").alias("temp_c_events"),
        F.size(F.collect_set("measure")).alias("num_measures"),
    )
    .filter(F.col("temp_c_events") > 1)
    .withColumn("val_p50_rank", F.rank().over(w))
    .orderBy(F.col("dv_04_val").desc())
)
