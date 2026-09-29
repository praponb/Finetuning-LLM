from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())
w2 = Window.orderBy("store_sale_revenue", "id")
w3 = Window.partitionBy("region").orderBy("store_sale_units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("city").orderBy("opened_date", "id")
w5 = Window.partitionBy("region").orderBy("sqft", "id")

with_latest = (
    df_stores
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.units").alias("store_sale_units"),
        F.col("c.revenue").alias("store_sale_revenue"),
    )
)

with_windows = (
    with_latest
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("running_sqft", F.sum("sqft").over(w3))
    .withColumn("prev_sqft", F.lag("sqft", 2).over(w4))
    .withColumn("seq_no", F.row_number().over(w5))
)

not_flagged = (
    with_windows
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.store_id").isin([3, 4, 6])), "left_anti")
)

cleaned = (
    not_flagged
    .filter(F.col("prev_sqft").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        "store_sale_units",
        "store_sale_revenue",
        "event_seq",
        "running_sqft",
        "prev_sqft",
        F.round((F.col("running_sqft") / F.col("event_seq")), 2).alias("running_sqft_per_event_seq"),
        F.trim("store_name").alias("clean_store_name"),
        F.coalesce("region", F.lit("NONE")).alias("resolved_region"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft",
        "store_sale_revenue",
        "running_sqft",
        "prev_sqft",
        "running_sqft_per_event_seq",
        F.when(F.col("region").rlike("^(South|West)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("region_zone"),
    )
    .orderBy(F.col("city").desc_nulls_first())
)
