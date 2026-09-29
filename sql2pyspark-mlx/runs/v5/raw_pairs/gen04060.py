from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at_period", "id")
w2 = Window.partitionBy("device").orderBy("started_at_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_sessions
    .filter(F.col("started_at") >= F.lit("2023-07-17").cast("date"))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "country",
        F.when(F.col("device").isin(["tablet", "desktop"]), "Core").otherwise("Other").alias("device_group"),
        F.date_trunc("MONTH", "started_at").alias("started_at_period"),
        F.coalesce("country", F.lit("UNKNOWN")).alias("country_clean"),
    )
)

with_gaps = (
    base
    .withColumn("is_new_session", F.when(F.lag("started_at_period").over(w).isNull() | (F.col("started_at_period") > F.lag("started_at_period").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_id", F.sum("is_new_session").over(w2))

result = (
    islands
    .cube("session_id", F.date_trunc("YEAR", "started_at_period").alias("year_start"))
    .agg(
        F.count(F.when(F.col("country") == "JP", 1)).alias("jp_count"),
        F.count(F.when(F.col("browser") == "Edge", 1)).alias("edge_count"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
    )
    .orderBy("year_start")
)
