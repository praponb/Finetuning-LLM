from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("device_code").orderBy("pressure", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.orderBy("reading_ts", "id")
w3 = Window.partitionBy("device_code")

enriched = (
    df_device_readings
    .select(
        "*",
        F.avg("pressure").over(w_hist).alias("pressure_avg_6"),
        F.row_number().over(w2).alias("row_seq"),
        F.corr("humidity", "pressure").over(w3).alias("corr_humidity_pressure"),
    )
)

result = (
    enriched
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        "firmware",
        "pressure_avg_6",
        "row_seq",
        "corr_humidity_pressure",
        F.when(F.col("device_code").rlike("^(DV-02|DV-04)$"), "GROUP_A").otherwise("GROUP_B").alias("device_code_zone"),
    )
    .orderBy(F.col("reading_ts").desc(), F.col("corr_humidity_pressure").desc())
)
