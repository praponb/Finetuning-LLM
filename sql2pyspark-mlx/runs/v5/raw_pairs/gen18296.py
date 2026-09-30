from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("humidity").desc(), "id")
w2 = Window.partitionBy("device_code").orderBy("status_bits")
w3 = Window.partitionBy("device_code").orderBy(F.col("temp_c").desc())
w_recent = Window.partitionBy("device_code")
w5 = Window.orderBy("device_code_stddev_samp_temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("temp_c", "id")
w7 = Window.orderBy("first_known_event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.orderBy("status_bits_rank", "id")

base = (
    df_device_readings
    .filter(~F.col("device_code").isin(["DV-02", "DV-04"]))
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "status_bits",
        "firmware",
        F.when(F.col("device_code").isin(["DV-04", "DV-02"]), "Priority").otherwise("Other").alias("device_code_group"),
    )
)

metrics = (
    base
    .filter(F.col("firmware") == "2.0.1")
    .select(
        "id",
        "temp_c",
        "status_bits",
        "device_code_group",
        F.row_number().over(w).alias("event_seq"),
        F.rank().over(w2).alias("status_bits_rank"),
        F.rank().over(w3).alias("temp_c_rank"),
        F.stddev_samp("temp_c").over(w_recent).alias("device_code_stddev_samp_temp_c"),
    )
)

metrics_device_readings = (
    metrics
    .select(
        "id",
        "temp_c",
        "status_bits",
        "device_code_group",
        "event_seq",
        "status_bits_rank",
        F.first("event_seq", ignorenulls=True).over(w5).alias("first_known_event_seq"),
        F.lead("temp_c", 2).over(w6).alias("next_temp_c"),
    )
)

enriched = (
    metrics_device_readings
    .withColumn("running_count_first_known_event_seq", F.count("first_known_event_seq").over(w7))
    .withColumn("running_min_first_known_event_seq", F.min("first_known_event_seq").over(w7))
    .withColumn("running_max_first_known_event_seq", F.max("first_known_event_seq").over(w7))
    .withColumn("prev_next_temp_c", F.lag("next_temp_c", 1, 0).over(w_ordered))
)

result = (
    enriched
    .select(
        "id",
        "temp_c",
        "status_bits",
        "device_code_group",
        "event_seq",
        "next_temp_c",
        "running_count_first_known_event_seq",
        "running_max_first_known_event_seq",
        "prev_next_temp_c",
    )
    .orderBy("running_count_first_known_event_seq")
)
