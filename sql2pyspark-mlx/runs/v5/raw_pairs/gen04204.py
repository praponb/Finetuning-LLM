from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country").orderBy(F.col("started_at").desc(), "id")

base = (
    df_sessions
    .filter(F.col("country") == "SG")
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.trim("browser").alias("clean_browser"),
        F.coalesce("country", F.lit("DIRECT")).alias("country_filled"),
    )
)

result = (
    base
    .select(
        "id",
        "customer_id",
        "browser",
        "country",
        F.last("country", ignorenulls=True).over(w).alias("last_known_country"),
        F.first("browser", ignorenulls=True).over(w2).alias("first_known_browser"),
        F.row_number().over(w3).alias("row_seq"),
    )
)
