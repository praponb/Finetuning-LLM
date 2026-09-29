from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.orderBy("sqft", "id")
w_recent = Window.partitionBy("city").orderBy("sqft", "id")
w4 = Window.partitionBy("city").orderBy(F.col("sqft").desc(), "id")
w5 = Window.orderBy("opened_date", "id")

with_windows = (
    df_stores
    .withColumn("sqft_share", F.round((F.col("sqft") / F.sum("sqft").over(w)), 4))
    .withColumn("next_sqft", F.lead("sqft", 1, 0).over(w2))
    .withColumn("seq_no", F.row_number().over(w_recent))
    .withColumn("row_seq", F.row_number().over(w4))
    .withColumn("next_sqft_2", F.lead("sqft", 2).over(w5))
)

cleaned = (
    with_windows
    .filter((F.col("opened_date") > F.lit("2023-07-26").cast("date")) & F.col("region").isNotNull())
    .withColumn("least_seq_no_next_sqft_2", F.least("seq_no", "next_sqft_2"))
)

base = (
    cleaned
    .filter((F.col("opened_date") >= F.lit("2023-12-20").cast("date")) & F.col("region").isNotNull() & (F.lower(F.trim("store_name")) == "airport kiosk"))
    .select(
        "id",
        "store_name",
        "region",
        "sqft",
        "sqft_share",
        "next_sqft",
        "seq_no",
        "next_sqft_2",
        "least_seq_no_next_sqft_2",
        F.when(F.col("city") == "Tokyo", 1).otherwise(0).alias("is_tokyo"),
        F.coalesce("region", F.lit("NONE")).alias("region_clean"),
    )
)

stats = (
    base
    .groupBy(
        F.when(F.col("next_sqft_2") >= 15225, "HIGH").otherwise("LOW").alias("next_sqft_2_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("region") == "North", 1)).alias("north_rows"),
    )
)

result = (
    stats
    .select("next_sqft_2_tier", "event_count")
    .orderBy("event_count", F.col("next_sqft_2_tier").desc())
)
