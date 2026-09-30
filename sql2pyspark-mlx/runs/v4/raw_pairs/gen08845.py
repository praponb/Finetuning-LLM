from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("humidity").asc_nulls_last())
w_grp = Window.partitionBy("device_code")
w3 = Window.orderBy("status_bits")
w4 = Window.partitionBy("device_code").orderBy(F.col("row_num").desc(), "id")
w5 = Window.partitionBy("device_code").orderBy("pressure", "id")
w6 = Window.partitionBy("device_code").orderBy(F.col("status_bits_dense_rank").desc_nulls_last())
w7 = Window.partitionBy("device_code").orderBy("row_num", "id")

deduped = (
    df_device_readings
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

metrics = (
    deduped
    .withColumn("corr_humidity_temp_c", F.corr("humidity", "temp_c").over(w_grp))
    .withColumn("device_code_stddev_samp_humidity", F.stddev_samp("humidity").over(w_grp))
    .withColumn("status_bits_dense_rank", F.dense_rank().over(w3))
)

latest = (
    metrics
    .withColumn("row_num_2", F.row_number().over(w4))
    .filter(F.col("row_num_2") == 1)
)

with_windows = (
    latest
    .withColumn("prev_status_bits_dense_rank", F.lag("status_bits_dense_rank", 2).over(w5))
    .withColumn("status_bits_dense_rank_cume_dist", F.cume_dist().over(w6))
    .withColumn("row_seq", F.row_number().over(w7))
)

result = (
    with_windows
    .groupBy("firmware")
    .pivot("device_code", ["DV-01", "DV-04"])
    .agg(F.avg("status_bits_dense_rank_cume_dist").alias("total"))
    .select("firmware", F.col("DV-01").alias("dv_01"), F.col("DV-04").alias("dv_04"))
)
