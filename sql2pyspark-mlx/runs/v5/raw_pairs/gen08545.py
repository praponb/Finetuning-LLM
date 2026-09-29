from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("browser").orderBy(F.col("started_at").desc(), "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id")
w3 = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_sessions
    .filter(F.col("started_at") > F.lit("2023-07-08 03:00:00").cast("timestamp"))
    .select(
        "id",
        "browser",
        "started_at",
        F.coalesce("country", F.lit("Unknown")).alias("country_filled"),
        F.coalesce("browser", F.lit("NONE")).alias("resolved_browser"),
    )
)

result = (
    cleaned
    .filter(F.col("started_at") < "2024-03-01")
    .select(
        "id",
        "browser",
        "started_at",
        "country_filled",
        F.row_number().over(w_part).alias("row_seq"),
        F.row_number().over(w2).alias("row_seq_2"),
        F.first("browser", ignorenulls=True).over(w3).alias("first_known_browser"),
    )
)
