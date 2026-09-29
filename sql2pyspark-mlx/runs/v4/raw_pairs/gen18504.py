from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("customer_id").orderBy("started_at_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_country").orderBy("started_at", "id")
w4 = Window.partitionBy("device").orderBy("started_at_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("event_time").desc(), "id")

valid = (
    df_sessions
    .filter((F.col("started_at") > F.lit("2023-06-01").cast("date")) & F.col("browser").isNotNull())
    .withColumn("started_at_date", F.to_date("started_at"))
)

with_dims = (
    valid
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.country").alias("customer_country"))
)

latest_match = (
    with_dims
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.event_time"))
)

enriched = (
    latest_match
    .select(
        "*",
        F.first("country", ignorenulls=True).over(w2).alias("first_known_country"),
        F.row_number().over(w3).alias("row_seq"),
        F.last("browser", ignorenulls=True).over(w4).alias("last_known_browser"),
        F.row_number().over(w5).alias("event_seq"),
    )
)

result = (
    enriched
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "country",
        "started_at_date",
        "customer_country",
        "event_time",
        "row_seq",
        F.when(F.col("device").rlike("^(tablet|desktop)$"), "PRIORITY").otherwise("GROUP_A").alias("device_zone"),
    )
    .orderBy(F.col("row_seq").asc_nulls_last())
)
