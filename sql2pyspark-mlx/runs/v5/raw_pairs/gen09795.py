from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc(), "id")

deduped = (
    df_device_readings
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    deduped
    .select(
        "id",
        "device_code",
        "humidity",
        "firmware",
        "rank_in_group",
        F.when(F.col("device_code").rlike("^(DV-04|DV-03)$"), "CORE").when(F.col("device_code").rlike("^(DV-01|DV-02)$"), "PRIORITY").otherwise("GROUP_A").alias("device_code_zone"),
    )
    .orderBy(F.col("firmware").desc(), F.col("device_code_zone").desc())
)
