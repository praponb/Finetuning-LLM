from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("country", "customer_id").orderBy(F.col("started_at").desc(), "id")
w3 = Window.partitionBy("device", "country").orderBy(F.col("started_at").desc(), "id")
w_running = Window.partitionBy("browser", "device").orderBy(F.col("started_at").desc(), "id")

metrics = (
    df_sessions
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.row_number().over(w2).alias("seq_no"),
        F.row_number().over(w3).alias("row_seq"),
        F.row_number().over(w_running).alias("seq_no_2"),
    )
)

result = (
    metrics
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        "event_seq",
        "seq_no",
        "row_seq",
    )
    .orderBy(F.col("started_at").desc())
)
