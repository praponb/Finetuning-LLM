from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "device").orderBy(F.col("started_at").desc(), "id")
w2 = Window.orderBy(F.col("started_at").desc(), "id")
w3 = Window.partitionBy("customer_id").orderBy("started_at", "id")

scored = (
    df_sessions
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.row_number().over(w2).alias("row_seq"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

result = (
    scored
    .groupBy(F.date_trunc("YEAR", "started_at").alias("year_start"))
    .agg(
        F.percentile_approx("seq_no", 0.9).alias("seq_no_p90"),
        F.count_if(F.col("country") == "DE").alias("de_events"),
        F.max("started_at").alias("last_seen"),
    )
    .filter(F.col("de_events") < 1)
    .orderBy(F.col("year_start").desc_nulls_last())
)
