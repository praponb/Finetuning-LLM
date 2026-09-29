from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("status_bits", "id")
w_grp = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(-2, Window.currentRow)
w_recent = Window.partitionBy("device_code").orderBy("humidity", "id")

base = (
    df_device_readings
    .filter(F.col("device_code").isin(["DV-01", "DV-04"]))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        F.date_format("reading_ts", "yyyy-MM").alias("reading_ts_label"),
    )
)

deduped = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "device_code", "humidity")
)

metrics = (
    deduped
    .select(
        "id",
        "humidity",
        F.sum("humidity").over(w_grp).alias("last3_sum_humidity"),
        F.min("humidity").over(w_grp).alias("last3_min_humidity"),
        F.max("humidity").over(w_grp).alias("last3_max_humidity"),
        F.avg("humidity").over(w_grp).alias("last3_avg_humidity"),
        F.lead("humidity", 2).over(w_recent).alias("next_humidity"),
    )
)

cleaned = (
    metrics
    .filter(F.col("last3_sum_humidity") >= 160)
    .select(
        "*",
        F.round((F.col("next_humidity") * 100), 2).alias("adjusted_next_humidity"),
        F.round("last3_min_humidity", 0).alias("last3_min_humidity_rounded"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("adjusted_next_humidity") >= 66, "LARGE").otherwise("SMALL").alias("adjusted_next_humidity_tier"),
        F.when(F.col("last3_max_humidity") >= 65, "PREMIUM").otherwise("STANDARD").alias("last3_max_humidity_tier"),
        F.when(F.col("last3_min_humidity_rounded") >= 77, "PREMIUM").otherwise("STANDARD").alias("last3_min_humidity_rounded_tier"),
    )
    .agg(
        F.percentile_approx("last3_max_humidity", [0.25, 0.5, 0.75]).alias("last3_max_humidity_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("last3_min_humidity"), 2).alias("avg_last3_min_humidity"),
        F.round(F.avg("humidity"), 2).alias("avg_humidity"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.percentile_approx("adjusted_next_humidity", 0.9).alias("adjusted_next_humidity_p90"),
    )
    .orderBy(F.col("avg_last3_min_humidity").asc_nulls_last())
)
