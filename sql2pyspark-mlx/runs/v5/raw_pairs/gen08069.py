from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.orderBy("reading_ts_quarter", "id")
w_ordered = Window.orderBy("humidity", "id")
w3 = Window.orderBy("status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("prev_humidity_filled", "id")

filtered = (
    df_device_readings
    .filter((F.col("reading_ts") >= F.to_date(F.lit("2025-09-01"))) & ~F.col("device_code").isin(["DV-02", "DV-01", "DV-03"]))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.col("status_bits").cast("string").alias("status_bits_num"),
        F.date_trunc("week", "reading_ts").alias("reading_ts_period"),
    )
)

filtered_device_readings = (
    filtered
    .filter(F.col("humidity") <= 57)
    .select(
        "id",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        "status_bits_num",
        "reading_ts_period",
        F.coalesce("humidity", F.lit(0)).alias("humidity_filled"),
        F.quarter("reading_ts").alias("reading_ts_quarter"),
    )
)

metrics = (
    filtered_device_readings
    .select(
        "id",
        "humidity",
        "pressure",
        "status_bits",
        "status_bits_num",
        "reading_ts_period",
        F.lag("humidity_filled").over(w_hist).alias("prev_humidity_filled"),
        F.lead("humidity_filled").over(w_hist).alias("next_humidity_filled"),
        F.lead("reading_ts_quarter").over(w_hist).alias("next_reading_ts_quarter"),
        F.row_number().over(w_hist).alias("row_seq"),
    )
)

scored = (
    metrics
    .select(
        "*",
        F.lag("next_humidity_filled").over(w_ordered).alias("prev_next_humidity_filled"),
        F.lag("pressure").over(w_ordered).alias("prev_pressure"),
        F.lead("pressure").over(w_ordered).alias("next_pressure"),
        F.row_number().over(w_ordered).alias("event_seq"),
        F.first("humidity", ignorenulls=True).over(w3).alias("first_known_humidity"),
    )
)

enriched = (
    scored
    .select(
        "id",
        "humidity",
        "status_bits_num",
        "reading_ts_period",
        "next_humidity_filled",
        "next_reading_ts_quarter",
        "row_seq",
        "prev_next_humidity_filled",
        "prev_pressure",
        "next_pressure",
        "event_seq",
        F.lead("humidity").over(w4).alias("next_humidity"),
        F.lag("humidity").over(w4).alias("prev_humidity"),
        F.lag("status_bits").over(w4).alias("prev_status_bits"),
        F.row_number().over(w4).alias("row_seq_2"),
    )
)

result = (
    enriched
    .select("id", "humidity", "next_pressure")
    .unpivot(
        ids=["id"],
        values=["humidity", "next_pressure"],
        variableColumnName="kpi",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
