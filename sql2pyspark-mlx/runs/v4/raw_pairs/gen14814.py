from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy(F.col("started_at").desc(), "id")
w_grp = Window.partitionBy("customer_id", "country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_sessions
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.last("country", ignorenulls=True).over(w_grp).alias("last_known_country"),
        F.last("browser", ignorenulls=True).over(w3).alias("last_known_browser"),
    )
)

result = (
    with_windows
    .groupBy(
        "seq_no",
        F.when(F.col("seq_no") >= 8, "LARGE").otherwise("SMALL").alias("seq_no_tier"),
    )
    .agg(
        F.round(F.sum("seq_no") / F.nullif(F.count(F.when(F.col("device") == "desktop", 1)), F.lit(0)), 2).alias("seq_no_per_desktop"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("span_minutes"),
        F.count("*").alias("cnt"),
    )
)
