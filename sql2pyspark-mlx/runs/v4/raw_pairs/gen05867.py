from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("sqft_whole", "id")
w2 = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy(F.col("sqft").desc(), "id")
w_grp = Window.partitionBy("city").orderBy(F.col("sqft_whole").desc_nulls_first())
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.discount").desc(), F.col("c.id").desc())

cleaned = (
    df_stores
    .filter((F.col("opened_date") > F.lit("2023-06-17").cast("date")) & F.col("region").isNotNull() & (F.col("sqft") != 5468))
    .select("id", "city", "sqft", F.ceil("sqft").alias("sqft_whole"))
)

deduped = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

metrics = (
    deduped
    .select(
        "*",
        F.sum("sqft").over(w2).alias("cumulative_sqft"),
        F.ntile(3).over(w_hist).alias("sqft_bucket"),
        F.dense_rank().over(w_grp).alias("sqft_whole_dense_rank"),
    )
)

with_latest = (
    metrics
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.status"), F.col("c.discount"))
)

stats = (
    with_latest
    .groupingSets([["city", "status"], ["city"], []], "city", "status")
    .agg(
        F.avg("cumulative_sqft").alias("avg_cumulative_sqft"),
        F.bit_and("sqft").alias("sqft_bit_and"),
        F.count_if(F.col("status") == "RETURNED").alias("returned_events"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.count_if(F.col("status") == "PENDING").alias("pending_events"),
        F.count("*").alias("cnt"),
        F.grouping_id().alias("grouping_id"),
    )
)

result = (
    stats
    .select(
        "city",
        "status",
        "sqft_bit_and",
        "returned_events",
        "avg_sqft",
        "pending_events",
        "cnt",
    )
)
