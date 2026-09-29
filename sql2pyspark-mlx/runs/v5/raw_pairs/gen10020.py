from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("device_code").orderBy("humidity", "id")
w2 = Window.partitionBy("device_code").orderBy("pressure", "id")
w3 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy("status_bits", "id")
w5 = Window.partitionBy("device_code").orderBy(F.col("pressure").desc(), "id")
w6 = Window.partitionBy("device_code").orderBy("humidity_plus_status_bits", "id")
w7 = Window.partitionBy("device_code").orderBy(F.col("row_seq_avg_30m").desc_nulls_last(), "id")
w8 = Window.partitionBy("device_code")
w_hist = Window.partitionBy("device_code").orderBy(F.col("next_humidity").desc_nulls_first())

cleaned = (
    df_device_readings
    .filter(F.col("status_bits") > 9)
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        (F.col("humidity") + F.col("status_bits")).alias("humidity_plus_status_bits"),
    )
)

enriched = (
    cleaned
    .select(
        "*",
        F.lead("humidity").over(w_ordered).alias("next_humidity"),
        F.lag("humidity").over(w_ordered).alias("prev_humidity"),
        F.lead("pressure").over(w_ordered).alias("next_pressure"),
        F.row_number().over(w_ordered).alias("row_seq"),
    )
)

metrics = (
    enriched
    .select(
        "*",
        F.lag("next_pressure", 2, 0).over(w2).alias("prev_next_pressure"),
        F.avg("row_seq").over(w3).alias("row_seq_avg_30m"),
        F.row_number().over(w4).alias("seq_no"),
        F.row_number().over(w5).alias("event_seq"),
        F.row_number().over(w6).alias("seq_no_2"),
    )
)

latest = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w7))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "device_code",
        "humidity",
        "status_bits",
        "humidity_plus_status_bits",
        "next_humidity",
        "prev_humidity",
        "row_seq",
    )
)

remaining_keys = (
    latest
    .select("id", "device_code")
    .subtract(
        df_device_readings
        .filter(F.col("firmware") == "1.1.0")
        .select("id", "device_code")
    )
)

kept_rows = (
    latest
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.device_code") == F.col("k.device_code")), "inner")
    .select("s.*")
)

enriched_device_readings = kept_rows.withColumn("device_code_avg_humidity", F.avg("humidity").over(w8))

enriched_device_readings_2 = (
    enriched_device_readings
    .filter(F.col("row_seq") < 7)
    .select(
        "id",
        "device_code",
        "row_seq",
        F.round((F.col("next_humidity") / F.sum("next_humidity").over(w8)), 4).alias("next_humidity_share"),
        F.percent_rank().over(w_hist).alias("next_humidity_percent_rank"),
    )
)

grouped = (
    enriched_device_readings_2
    .groupBy("device_code")
    .agg(
        F.count(F.when(F.col("device_code") == "DV-02", 1)).alias("dv_02_rows"),
        F.percentile_approx("row_seq", [0.25, 0.5, 0.75]).alias("row_seq_quartiles"),
    )
)

result = (
    grouped
    .select("device_code", "dv_02_rows", "row_seq_quartiles")
    .orderBy("device_code")
)
