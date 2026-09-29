from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("sqft").desc(), "id")
w2 = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(-2, Window.currentRow)

cleaned = (
    df_stores
    .filter((F.col("opened_date") > F.lit("2023-07-05").cast("date")) & F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "opened_date",
        "sqft",
        F.lower("store_name").alias("store_name_lower"),
    )
)

eligible = (
    cleaned
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.order_date") < F.col("s.opened_date")), "left_semi")
)

with_windows = (
    eligible
    .withColumn("sqft_bucket", F.ntile(4).over(w))
    .withColumn("sqft_avg_2", F.avg("sqft").over(w2))
)

result = (
    with_windows
    .groupBy("city")
    .agg(
        F.avg("sqft_bucket").alias("avg_sqft_bucket"),
        F.round(F.avg("sqft_avg_2"), 2).alias("avg_sqft_avg_2"),
        F.stddev_samp("sqft").alias("sqft_stddev"),
    )
    .orderBy(F.col("avg_sqft_avg_2").desc_nulls_last())
)
