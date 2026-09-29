from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("started_at_day").desc_nulls_first(), "id")
w2 = Window.orderBy("rn", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country").orderBy("started_at_day", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_sessions
    .filter(F.col("customer_id").isNotNull() & F.col("browser").isin(["Safari", "Edge"]))
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        "country",
        F.col("started_at").cast("date").alias("started_at_day"),
        F.trim("country").alias("clean_country"),
        F.coalesce("browser", F.lit("NONE")).alias("resolved_browser"),
    )
)

deduped = (
    base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

enriched = (
    deduped
    .select(
        "*",
        F.first("country", ignorenulls=True).over(w2).alias("first_known_country"),
        F.last("country", ignorenulls=True).over(w3).alias("last_known_country"),
        F.first("country", ignorenulls=True).over(w4).alias("first_known_country_2"),
    )
)

valid = (
    enriched
    .filter(F.col("started_at") >= F.to_timestamp(F.lit("2023-11-02 22:00:00")))
    .select(
        "id",
        "device",
        "country",
        "started_at_day",
        "clean_country",
        "resolved_browser",
        "rn",
        "first_known_country",
        "first_known_country_2",
        F.ceil("rn").alias("rn_whole"),
        F.datediff(F.current_date(), "started_at_day").alias("days_since_started_at_day"),
        F.coalesce("browser", F.lit("DIRECT")).alias("resolved_browser_2"),
    )
)

result = (
    valid
    .select(
        "id",
        "device",
        "country",
        "started_at_day",
        "clean_country",
        "rn",
        "first_known_country",
        "rn_whole",
        "days_since_started_at_day",
    )
)
