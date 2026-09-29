from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("device_code").orderBy("humidity", "id")
w2 = Window.partitionBy("device_code").orderBy(F.col("temp_c").desc(), "id")

enriched = (
    df_device_readings
    .select(
        "id",
        "device_code",
        "humidity",
        "status_bits",
        F.lag("status_bits").over(w_hist).alias("prev_status_bits"),
        F.lead("temp_c").over(w_hist).alias("next_temp_c"),
        F.lag("temp_c").over(w_hist).alias("prev_temp_c"),
        F.row_number().over(w_hist).alias("seq_no"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

result = (
    enriched
    .select(
        "id",
        "device_code",
        "humidity",
        "status_bits",
        "prev_status_bits",
        "next_temp_c",
        "prev_temp_c",
        "seq_no",
        "row_seq",
        F.when(F.col("device_code").rlike("^(DV-02|DV-04)$"), "EXTENDED").otherwise("PRIORITY").alias("device_code_zone"),
    )
    .orderBy("seq_no", F.col("status_bits").desc())
)
