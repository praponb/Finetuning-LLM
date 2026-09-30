from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.orderBy("started_at", "id")
w_running_2 = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc(), "id")

enriched = (
    df_sessions
    .withColumn("first_known_browser", F.first("browser", ignorenulls=True).over(w))
    .withColumn("last_known_country", F.last("country", ignorenulls=True).over(w2))
    .withColumn("event_seq", F.row_number().over(w_running))
    .withColumn("seq_no", F.row_number().over(w_running_2))
)

result = (
    enriched
    .select(
        "id",
        "customer_id",
        "device",
        "first_known_browser",
        "last_known_country",
        "event_seq",
        "seq_no",
        F.when(F.col("device").rlike("^(mobile|tablet)$"), "EXTENDED").when(F.col("device").rlike("^(desktop)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("device_zone"),
    )
)
