from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy("units", "id")
w2 = Window.partitionBy("store_id").orderBy(F.col("units").desc_nulls_last(), "id")
w3 = Window.partitionBy("store_id").orderBy(F.col("avg_days_since_sale_date").desc())

valid = (
    df_store_sales
    .filter((F.col("sale_date") > F.lit("2023-04-04").cast("date")) & (F.year("sale_date") >= 2023))
    .select("*", (F.col("revenue") * F.col("units")).alias("revenue_per_unit"))
)

with_windows = (
    valid
    .withColumn("next_units", F.lead("units").over(w))
    .withColumn("prev_units", F.lag("units").over(w))
    .withColumn("next_revenue_per_unit", F.lead("revenue_per_unit").over(w))
    .withColumn("prev_revenue_per_unit", F.lag("revenue_per_unit").over(w))
    .withColumn("event_seq", F.row_number().over(w))
)

base = (
    with_windows
    .filter((F.col("sale_date") > F.lit("2023-08-31").cast("date")) & (F.col("next_revenue_per_unit") < 47))
    .select(
        "id",
        "store_id",
        "sale_date",
        "units",
        "revenue",
        "revenue_per_unit",
        "prev_units",
        "prev_revenue_per_unit",
        "event_seq",
        F.datediff(F.current_date(), "sale_date").alias("days_since_sale_date"),
        F.ceil("prev_units").alias("prev_units_whole"),
    )
)

ranked = (
    base
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked
    .groupBy("store_id", F.date_trunc("MONTH", "sale_date").alias("activity_month"))
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.avg("days_since_sale_date"), 2).alias("avg_days_since_sale_date"),
        F.round(F.avg("event_seq"), 2).alias("avg_event_seq"),
        F.round(F.avg("event_seq"), 2).alias("avg_event_seq_2"),
    )
    .withColumn("avg_days_since_sale_date_rank", F.rank().over(w3))
)
