from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("status_bits", "id")
w2 = Window.partitionBy("device_code").orderBy("temp_c", "id")
w3 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)

base = (
    df_device_readings
    .filter((F.col("reading_ts") >= F.lit("2025-09-01 02:00:00").cast("timestamp")) & (F.col("pressure") < 1007))
    .withColumn("status_bits_times_humidity", (F.col("status_bits") * F.col("humidity")))
)

ranked = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

metrics = (
    ranked
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        "latest_rank",
        F.lag("status_bits").over(w2).alias("prev_status_bits"),
        F.lead("status_bits").over(w2).alias("next_status_bits"),
        F.lead("status_bits_times_humidity").over(w2).alias("next_status_bits_times_humidity"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

kept_keys = (
    metrics
    .select("firmware", "device_code")
    .subtract(
        df_device_readings
        .filter(F.col("device_code") == "DV-03")
        .select("firmware", "device_code")
    )
)

verified = (
    metrics
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.firmware") == F.col("k.firmware")) & (F.col("s.device_code") == F.col("k.device_code")), "inner")
    .select("s.*")
)

scored = (
    verified
    .select(
        "device_code",
        "reading_ts",
        "latest_rank",
        "next_status_bits_times_humidity",
        F.count("prev_status_bits").over(w3).alias("prev_status_bits_count_1h"),
        F.avg("prev_status_bits").over(w4).alias("prev_status_bits_avg_2h"),
    )
)

valid = (
    scored
    .filter(F.col("device_code") != "DV-01")
    .select(
        "*",
        F.when(F.col("prev_status_bits_count_1h") >= 13, "High").when(F.col("prev_status_bits_count_1h") >= 6, "Medium").otherwise("Low").alias("prev_status_bits_count_1h_band"),
    )
)

stats = (
    valid
    .withColumn("prev_status_bits_count_1h_tier", F.when(F.col("prev_status_bits_count_1h") >= 14, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["device_code", "prev_status_bits_count_1h_tier"], ["device_code"]],
        "device_code",
        "prev_status_bits_count_1h_tier",
    )
    .agg(
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
        F.round(F.sum("next_status_bits_times_humidity") / F.nullif(F.count(F.when(F.col("device_code") == "DV-02", 1)), F.lit(0)), 2).alias("next_status_bits_times_humidity_per_dv_02"),
        F.countDistinct("device_code").alias("distinct_device_codes"),
        F.round(F.sum("prev_status_bits_count_1h") / F.nullif(F.count(F.when(F.col("device_code") == "DV-04", 1)), F.lit(0)), 2).alias("prev_status_bits_count_1h_per_dv_04"),
        F.sum(F.when(F.col("device_code") == "DV-02", F.col("next_status_bits_times_humidity")).otherwise(0)).alias("dv_02_next_status_bits_times_humidity"),
        F.grouping("device_code").alias("grouping_level"),
    )
)

result = (
    stats
    .select(
        "device_code",
        "prev_status_bits_count_1h_tier",
        "device_code_list",
        "next_status_bits_times_humidity_per_dv_02",
        "distinct_device_codes",
        "prev_status_bits_count_1h_per_dv_04",
        "grouping_level",
        F.when(F.col("device_code").rlike("^(DV-03|DV-02)$"), "PRIORITY").otherwise("GROUP_A").alias("device_code_zone"),
    )
    .orderBy(
        F.col("prev_status_bits_count_1h_per_dv_04").asc_nulls_first(),
        "prev_status_bits_count_1h_tier",
    )
)
