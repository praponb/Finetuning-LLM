from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy(F.col("humidity").desc_nulls_first())
w3 = Window.partitionBy("device_code").orderBy(F.col("pressure").desc(), "id")
w_running = Window.partitionBy("device_code").orderBy("humidity", "id")
w5 = Window.orderBy(F.col("first_known_humidity").desc(), "id")
w6 = Window.partitionBy("device_code")

cleaned = (
    df_device_readings
    .filter(F.col("reading_ts") > F.to_timestamp(F.lit("2025-09-01 07:00:00")))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "firmware",
        F.upper("device_code").alias("device_code_upper"),
    )
)

metrics = (
    cleaned
    .select(
        "*",
        F.first("humidity", ignorenulls=True).over(w).alias("first_known_humidity"),
        F.dense_rank().over(w2).alias("humidity_dense_rank"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

enriched = (
    metrics
    .withColumn("next_first_known_humidity", F.lead("first_known_humidity").over(w_running))
    .withColumn("next_humidity", F.lead("humidity").over(w_running))
    .withColumn("row_seq", F.row_number().over(w_running))
)

with_windows = (
    enriched
    .withColumn("seq_no_2", F.row_number().over(w5))
    .withColumn("corr_humidity_pressure", F.corr("humidity", "pressure").over(w6))
    .withColumn("pressure_bucket", F.ntile(10).over(w3))
)

result = (
    with_windows
    .select(
        "id",
        "device_code",
        "reading_ts",
        "pressure",
        "firmware",
        "device_code_upper",
        "first_known_humidity",
        "humidity_dense_rank",
        "seq_no",
        F.when(F.col("device_code").rlike("^(DV-01|DV-02)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("device_code_zone"),
    )
    .orderBy(F.col("pressure").desc_nulls_first())
)
