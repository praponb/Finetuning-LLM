from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country", "browser").orderBy("started_at", "id")
w3 = Window.partitionBy("browser").orderBy(F.col("started_at").desc(), "id")

scored = (
    df_sessions
    .select(
        "id",
        "browser",
        "started_at",
        F.first("country", ignorenulls=True).over(w).alias("first_known_country"),
        F.row_number().over(w2).alias("seq_no"),
        F.row_number().over(w3).alias("row_seq"),
    )
)

result = (
    scored
    .groupBy(
        F.date_trunc("MONTH", "started_at").alias("activity_month"),
        "seq_no",
        F.when(F.col("seq_no") >= 7, "HIGH").otherwise("LOW").alias("seq_no_tier"),
    )
    .agg(
        F.max("started_at").alias("last_started_at"),
        F.max("started_at").alias("last_started_at_2"),
        F.max("started_at").alias("last_seen"),
    )
)
