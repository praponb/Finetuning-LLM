from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("duration_sec").asc_nulls_first())
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("page").orderBy(F.col("event_time").desc(), "id")

cleaned = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .withColumn("event_type_group", F.when(F.col("event_type").isin(["search", "click"]), "Priority").otherwise("Other"))
)

result = (
    cleaned
    .select(
        "id",
        "event_type",
        "event_time",
        "event_type_group",
        F.percent_rank().over(w).alias("duration_sec_percent_rank"),
        F.avg("duration_sec").over(w2).alias("duration_sec_avg_5_prior"),
        F.row_number().over(w3).alias("row_seq"),
    )
)
