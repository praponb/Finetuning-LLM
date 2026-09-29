from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("browser", "country").orderBy(F.col("started_at").desc(), "id")
w3 = Window.partitionBy("browser", "customer_id").orderBy(F.col("started_at").desc(), "id")
w_running_2 = Window.partitionBy("device").orderBy(F.col("started_at").desc(), "id")
w5 = Window.partitionBy("device").orderBy(F.col("seq_no_2").desc(), "id")
w6 = Window.orderBy("seq_no_3", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("device")

eligible = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") < F.col("s.started_at")), "left_anti")
)

scored = (
    eligible
    .select(
        "*",
        F.first("country", ignorenulls=True).over(w).alias("first_known_country"),
        F.row_number().over(w_running).alias("seq_no"),
        F.row_number().over(w3).alias("seq_no_2"),
        F.row_number().over(w_running_2).alias("seq_no_3"),
    )
)

enriched = (
    scored
    .select(
        "*",
        F.row_number().over(w5).alias("row_seq"),
        F.first("country", ignorenulls=True).over(w6).alias("first_known_country_2"),
        F.round((F.col("seq_no") / F.sum("seq_no").over(w7)), 4).alias("seq_no_share"),
    )
)

wide = (
    enriched
    .groupBy("customer_id")
    .pivot("device", ["mobile", "tablet"])
    .agg(F.avg("seq_no_share").alias("total"))
)

result = (
    wide
    .select("customer_id", "mobile", "tablet")
    .orderBy(F.col("mobile").desc_nulls_first())
)
