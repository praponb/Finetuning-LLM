from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("temp_c").desc())
w2 = Window.orderBy("status_bits", "id")
w3 = Window.orderBy("pressure", "id")
w4 = Window.partitionBy("device_code")

deduped = (
    df_device_readings
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

metrics = (
    deduped
    .withColumn("prev_temp_c", F.lag("temp_c", 1).over(w2))
    .withColumn("prev_status_bits", F.lag("status_bits", 1).over(w3))
    .withColumn("device_code_min_humidity", F.min("humidity").over(w4))
)

matched_keys = (
    metrics
    .select("device_code", "id")
    .intersectAll(
        df_device_readings
        .filter(F.col("temp_c") >= 28)
        .select("device_code", "id")
    )
)

kept_rows = (
    metrics
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.device_code") == F.col("k.device_code")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

remaining_keys = (
    kept_rows
    .select("id", "firmware")
    .subtract(
        df_device_readings
        .filter(~F.col("device_code").isin(["DV-03", "DV-01"]))
        .select("id", "firmware")
    )
)

reconciled = (
    kept_rows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.firmware") == F.col("k.firmware")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.when(F.col("temp_c") >= 34.7, "HIGH").otherwise("LOW").alias("temp_c_tier"),
        F.when(F.col("humidity") >= 67, "LARGE").otherwise("SMALL").alias("humidity_tier"),
    )
    .agg(
        F.countDistinct("firmware").alias("distinct_firmwares"),
        F.bit_and("status_bits").alias("status_bits_bit_and"),
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("span_minutes"),
    )
)
