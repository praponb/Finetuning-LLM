from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy("opened_date", "id")
w2 = Window.partitionBy("region").orderBy("opened_date_quarter", "id")
w3 = Window.partitionBy("region").orderBy("sqft", "id")
w4 = Window.orderBy(F.col("opened_date_quarter_bucket").desc(), "id")
w5 = Window.orderBy("sqft", "id")

base = (
    df_stores
    .filter((F.col("opened_date") >= F.lit("2023-04-25").cast("date")) & F.col("region").isin(["East", "West"]))
    .select(
        "*",
        F.quarter("opened_date").alias("opened_date_quarter"),
        F.when(F.col("region").isin(["North", "South"]), "Group A").otherwise("Other").alias("region_group"),
        F.coalesce("region", F.lit("DIRECT")).alias("resolved_region"),
    )
)

latest = (
    base
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "store_name",
        "region",
        "opened_date",
        "sqft",
        "opened_date_quarter",
        "region_group",
    )
)

with_windows = (
    latest
    .filter(F.col("sqft").between(11679, 12848))
    .select(
        "id",
        "sqft",
        "region_group",
        F.ntile(5).over(w2).alias("opened_date_quarter_bucket"),
        F.lag("opened_date_quarter", 2).over(w3).alias("prev_opened_date_quarter"),
    )
)

with_windows_stores = (
    with_windows
    .select(
        "*",
        F.ntile(10).over(w4).alias("opened_date_quarter_bucket_bucket"),
        F.lag("opened_date_quarter_bucket", 1).over(w5).alias("prev_opened_date_quarter_bucket"),
        F.row_number().over(w4).alias("event_seq"),
    )
)

result = (
    with_windows_stores
    .select(
        "id",
        "sqft",
        "opened_date_quarter_bucket",
        "prev_opened_date_quarter_bucket",
        "event_seq",
    )
    .orderBy(F.col("id").desc())
    .limit(5)
)
