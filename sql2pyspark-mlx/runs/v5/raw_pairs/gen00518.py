from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc(), "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w3 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy("status_bits")
w5 = Window.orderBy(F.col("island_id").desc(), "id")
w6 = Window.partitionBy("device_code")
w7 = Window.partitionBy("device_code").orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_device_readings
    .filter(F.col("status_bits") > 8)
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.datediff(F.current_date(), "reading_ts").alias("days_since_reading_ts"),
    )
)

latest = (
    valid
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

flagged = (
    latest
    .withColumn("is_new_session", F.when(F.lag("reading_ts").over(w2).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w2) + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

sessions_device_readings = flagged.withColumn("island_id", F.sum("is_new_session").over(w3))

deduped = (
    sessions_device_readings
    .withColumn("rank_in_group", F.rank().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

scored = (
    deduped
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "status_bits",
        "firmware",
        "is_new_session",
        "island_id",
        F.row_number().over(w5).alias("row_seq"),
        F.round((F.col("temp_c") / F.sum("temp_c").over(w6)), 4).alias("temp_c_share"),
        F.sum("pressure").over(w7).alias("cumulative_pressure"),
    )
)

result = (
    scored
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "status_bits",
        "firmware",
        "island_id",
        "temp_c_share",
        "cumulative_pressure",
        F.when(F.col("device_code").rlike("^(DV-01|DV-04)$"), "PRIORITY").when(F.col("device_code").rlike("^(DV-02|DV-03)$"), "OTHER_ZONE").otherwise("CORE").alias("device_code_zone"),
    )
    .orderBy(F.col("device_code").desc_nulls_first(), "humidity")
)
