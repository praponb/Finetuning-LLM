from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc(), "id")
w3 = Window.partitionBy("customer_id", "device").orderBy(F.col("started_at").desc(), "id")

enriched = (
    df_sessions
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("event_seq", F.row_number().over(w3))
)

result = (
    enriched
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "row_seq",
        "event_seq",
        F.when(F.col("device").rlike("^(desktop|mobile)$"), "GROUP_A").otherwise("GROUP_B").alias("device_zone"),
    )
    .orderBy(F.col("device_zone").desc_nulls_first())
)
