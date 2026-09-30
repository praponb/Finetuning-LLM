from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("humidity_whole").desc_nulls_first())
w2 = Window.partitionBy("device_code").orderBy(F.col("temp_c").desc(), "id")
w3 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy(F.col("humidity").desc())
w5 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(-2, -1)

filtered = (
    df_device_readings
    .filter(F.col("temp_c") <= 30)
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        F.floor("humidity").alias("humidity_whole"),
        F.lower("device_code").alias("device_code_lower"),
    )
)

result = (
    filtered
    .withColumn("humidity_whole_rank", F.rank().over(w))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("humidity_whole_avg_2h", F.avg("humidity_whole").over(w3))
    .withColumn("humidity_dense_rank", F.dense_rank().over(w4))
    .withColumn("humidity_whole_avg_2_prior", F.avg("humidity_whole").over(w5))
)
