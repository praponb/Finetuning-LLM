from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("status_bits", "id")
w2 = Window.partitionBy("device_code").orderBy("humidity", "id")
w3 = Window.orderBy(F.col("seq_no").desc_nulls_first())
w_grp = Window.orderBy(F.col("prev_pressure").asc_nulls_first())
w5 = Window.orderBy(F.col("prev_pressure").desc_nulls_last())

scored = (
    df_device_readings
    .filter(F.col("status_bits") >= 6)
    .select(
        "id",
        "firmware",
        F.row_number().over(w).alias("seq_no"),
        F.lag("pressure", 1).over(w2).alias("prev_pressure"),
    )
)

cleaned = (
    scored
    .filter(F.col("prev_pressure") < 1021)
    .select("*", F.coalesce("prev_pressure", F.lit(0)).alias("prev_pressure_filled"))
)

matched_keys = (
    cleaned
    .select("firmware", "id")
    .subtract(
        df_device_readings
        .filter(F.col("humidity") > 50)
        .select("firmware", "id")
    )
)

reconciled = (
    cleaned
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.firmware") == F.col("k.firmware")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .withColumn("seq_no_percent_rank", F.percent_rank().over(w3))
    .withColumn("prev_pressure_percent_rank", F.percent_rank().over(w_grp))
)

metrics = (
    with_windows
    .select(
        "firmware",
        "seq_no",
        "prev_pressure",
        "prev_pressure_percent_rank",
        F.rank().over(w5).alias("prev_pressure_rank"),
    )
)

result = (
    metrics
    .select("firmware", "seq_no", "prev_pressure_rank")
    .orderBy(F.col("firmware").asc_nulls_first(), "prev_pressure_rank")
)
