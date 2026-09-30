from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("status_bits", "id")
w2 = Window.partitionBy("device_code")
w3 = Window.partitionBy("device_code").orderBy("pressure", "id")
w4 = Window.partitionBy("device_code").orderBy(F.col("next_status_bits").asc_nulls_last(), "id")
w5 = Window.orderBy("device_code_count_status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("device_code").orderBy("event_seq", "id")

metrics = (
    df_device_readings
    .filter(F.col("firmware") == "1.1.0")
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "status_bits",
        F.lead("status_bits", 1).over(w).alias("next_status_bits"),
        F.count("status_bits").over(w2).alias("device_code_count_status_bits"),
        F.row_number().over(w3).alias("event_seq"),
    )
)

deduped = (
    metrics
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

scored = (
    deduped
    .select(
        "id",
        "temp_c",
        "next_status_bits",
        "rank_in_group",
        F.last("event_seq", ignorenulls=True).over(w5).alias("last_known_event_seq"),
        F.row_number().over(w6).alias("row_seq"),
    )
)

valid = (
    scored
    .filter(F.col("next_status_bits") >= 9)
    .select(
        "id",
        "temp_c",
        "rank_in_group",
        "last_known_event_seq",
        "row_seq",
        F.floor("last_known_event_seq").alias("last_known_event_seq_whole"),
    )
)

valid_device_readings = (
    valid
    .filter(F.col("temp_c") < 26)
    .select(
        "*",
        F.when(F.col("row_seq") >= 8, "High").when(F.col("row_seq") >= 4, "Medium").otherwise("Low").alias("row_seq_band"),
        F.floor("rank_in_group").alias("rank_in_group_whole"),
    )
)

result = (
    valid_device_readings
    .select("id", "temp_c", "last_known_event_seq", "row_seq", "row_seq_band")
    .orderBy(F.col("id").asc_nulls_first())
)
