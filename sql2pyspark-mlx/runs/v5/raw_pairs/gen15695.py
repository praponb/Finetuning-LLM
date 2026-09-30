from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("device_code").orderBy("status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.orderBy("running_sum_pressure", "id").rowsBetween(-6, -1)
w3 = Window.orderBy("running_max_pressure", "id")
w4 = Window.orderBy("status_bits", "id")
w_part = Window.orderBy("seq_no", "id")
w6 = Window.partitionBy("event_seq_tier").orderBy(F.col("pressure_p90").desc())

scored = (
    df_device_readings
    .select(
        "id",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.max("pressure").over(w_grp).alias("running_max_pressure"),
        F.sum("pressure").over(w_grp).alias("running_sum_pressure"),
        F.count("pressure").over(w_grp).alias("running_count_pressure"),
    )
)

scored_device_readings = (
    scored
    .select(
        "*",
        F.avg("temp_c").over(w_ordered).alias("temp_c_avg_6_prior"),
        F.row_number().over(w3).alias("seq_no"),
        F.row_number().over(w4).alias("event_seq"),
    )
)

base = (
    scored_device_readings
    .filter(F.col("temp_c").between(33, 36))
    .select(
        "id",
        "temp_c",
        "humidity",
        "pressure",
        "firmware",
        "running_max_pressure",
        "running_sum_pressure",
        "seq_no",
        "event_seq",
        F.col("pressure").cast("int").alias("pressure_int"),
    )
)

metrics = (
    base
    .select(
        "id",
        "humidity",
        "pressure",
        "running_max_pressure",
        "running_sum_pressure",
        "seq_no",
        "event_seq",
        "pressure_int",
        F.lag("humidity", 2).over(w_part).alias("prev_humidity"),
        F.lead("temp_c", 1).over(w_part).alias("next_temp_c"),
    )
)

matched_keys = (
    metrics
    .select("id")
    .subtract(
        df_device_readings
        .filter(F.col("humidity") <= 74)
        .select("id")
    )
)

reconciled = (
    metrics
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.when(F.col("event_seq") >= 9, "LARGE").otherwise("SMALL").alias("event_seq_tier"),
        "seq_no",
    )
    .agg(
        F.max("running_sum_pressure").alias("max_running_sum_pressure"),
        F.percentile_approx("pressure", 0.9).alias("pressure_p90"),
        F.count("*").alias("num_rows"),
    )
    .filter(F.col("pressure_p90") >= 0)
    .withColumn("pressure_p90_rank", F.rank().over(w6))
)
