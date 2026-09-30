from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("started_at").desc(), "id")
w_grp = Window.partitionBy("browser").orderBy("started_at", "id")
w3 = Window.partitionBy("device", "customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("started_at", "id")

scored = (
    df_sessions
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.row_number().over(w_grp).alias("seq_no"),
        F.last("country", ignorenulls=True).over(w3).alias("last_known_country"),
        F.last("country", ignorenulls=True).over(w4).alias("last_known_country_2"),
        F.row_number().over(w5).alias("event_seq_2"),
    )
)

result = (
    scored
    .groupBy(
        "device",
        F.when(F.col("seq_no") >= 7, "LARGE").otherwise("SMALL").alias("seq_no_tier"),
    )
    .agg(
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min"),
        F.count("*").alias("row_count"),
        F.percentile_approx("event_seq", [0.25, 0.5, 0.75]).alias("event_seq_quartiles"),
    )
    .filter(F.col("duration_min") > 0)
)
