from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.order_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city").orderBy("sqft", "id")
w3 = Window.partitionBy("region")
w_recent = Window.partitionBy("order_status").orderBy("region_min_sqft", "id")
w5 = Window.partitionBy("order_status").orderBy(F.col("sqft_bucket").desc(), "id")
w_ordered = Window.orderBy("region_min_sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("city").orderBy("sqft_bucket", "id")
w_part = Window.partitionBy("region").orderBy(F.col("sqft_bucket").desc(), "id")
w9 = Window.partitionBy("region").orderBy("running_region_min_sqft", "id")

filtered = (
    df_stores
    .filter((F.col("opened_date") >= F.to_date(F.lit("2023-07-09"))) & F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft",
        F.col("sqft").cast("string").alias("sqft_num"),
        F.lower("city").alias("city_lower"),
    )
)

with_latest = (
    filtered
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.status").alias("order_status"))
)

scored = (
    with_latest
    .withColumn("sqft_bucket", F.ntile(5).over(w2))
    .withColumn("region_min_sqft", F.min("sqft").over(w3))
)

metrics = (
    scored
    .withColumn("region_min_sqft_bucket", F.ntile(10).over(w_recent))
    .withColumn("row_seq", F.row_number().over(w5))
    .withColumn("running_region_min_sqft", F.sum("region_min_sqft").over(w_ordered))
    .withColumn("prev_region_min_sqft", F.lag("region_min_sqft", 1).over(w7))
    .withColumn("sqft_bucket_bucket", F.ntile(4).over(w_part))
)

result = (
    metrics
    .select(
        "id",
        "store_name",
        "region",
        "sqft_num",
        "city_lower",
        "order_status",
        "region_min_sqft",
        "region_min_sqft_bucket",
        F.lead("running_region_min_sqft").over(w9).alias("next_running_region_min_sqft"),
        F.lead("sqft").over(w9).alias("next_sqft"),
        F.lag("sqft").over(w9).alias("prev_sqft"),
        F.row_number().over(w9).alias("row_seq_2"),
    )
)
