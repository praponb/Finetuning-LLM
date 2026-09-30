from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("humidity").desc(), "id")
w2 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w3 = Window.orderBy("pressure", "id").rowsBetween(-3, -1)
w4 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w5 = Window.orderBy("pressure", "id").rowsBetween(-5, -1)
w6 = Window.orderBy(F.col("pressure_avg_3_prior").desc(), "id")
w7 = Window.orderBy("pressure_avg_3_prior", "id")
w8 = Window.orderBy("pressure_avg_3_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("pressure_avg_3_prior_count_15m", "id")
w10 = Window.orderBy(F.col("pressure_avg_3_prior_count_15m").desc_nulls_last())
w11 = Window.orderBy(F.col("seq_no").desc(), "id")

deduped = (
    df_device_readings
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "reading_ts", "pressure", "firmware")
)

scored = (
    deduped
    .filter(F.col("firmware").isin(["2.0.1", "1.1.0"]))
    .select(
        "*",
        F.count("pressure").over(w2).alias("pressure_count_30m"),
        F.avg("pressure").over(w3).alias("pressure_avg_3_prior"),
    )
)

scored_device_readings = (
    scored
    .select(
        "id",
        "firmware",
        "pressure_avg_3_prior",
        F.count("pressure_avg_3_prior").over(w4).alias("pressure_avg_3_prior_count_15m"),
        F.avg("pressure_count_30m").over(w5).alias("pressure_count_30m_avg_5_prior"),
        F.row_number().over(w6).alias("seq_no"),
    )
)

metrics = (
    scored_device_readings
    .withColumn("pressure_avg_3_prior_bucket", F.ntile(3).over(w7))
    .withColumn("first_known_pressure_avg_3_prior_count_15m", F.first("pressure_avg_3_prior_count_15m", ignorenulls=True).over(w8))
    .withColumn("seq_no_2", F.row_number().over(w9))
    .withColumn("pressure_avg_3_prior_count_15m_rank", F.rank().over(w10))
    .withColumn("seq_no_bucket", F.ntile(3).over(w11))
)

cleaned = (
    metrics
    .filter(F.col("pressure_avg_3_prior_count_15m") >= 9)
    .select(
        "id",
        "firmware",
        "pressure_avg_3_prior",
        "pressure_avg_3_prior_count_15m",
        "seq_no",
        "pressure_avg_3_prior_count_15m_rank",
        "seq_no_bucket",
        F.when(F.col("firmware") == "1.1.0", 1).otherwise(0).alias("is_1_1_0"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "firmware",
        "pressure_avg_3_prior",
        "pressure_avg_3_prior_count_15m",
        "seq_no",
        "pressure_avg_3_prior_count_15m_rank",
        "seq_no_bucket",
        "is_1_1_0",
    )
    .orderBy("firmware", "id")
)
