from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("sqft_num").desc(), "id")
w2 = Window.partitionBy("region").orderBy(F.col("sqft_num").desc_nulls_last())
w3 = Window.orderBy("opened_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("region").orderBy("sqft", "id")
w5 = Window.partitionBy("region").orderBy("sqft_num", "id")
w6 = Window.partitionBy("region").orderBy(F.col("sqft_num").desc_nulls_first())
w7 = Window.orderBy(F.col("opened_date_period").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w8 = Window.partitionBy("region").orderBy(F.col("cumulative_sqft").desc_nulls_first())
w9 = Window.partitionBy("region").orderBy("opened_date", "id")

cleaned = (
    df_stores
    .filter(~F.col("store_name").like("O%"))
    .select(
        "id",
        "store_name",
        "region",
        "opened_date",
        "sqft",
        F.concat_ws("-", "region", "store_name").alias("region_store_name_key"),
        F.col("sqft").cast("double").alias("sqft_num"),
    )
)

deduped = (
    cleaned
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

filtered = (
    deduped
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-12-26"))) & F.col("region").isNotNull() & (F.col("sqft_num") >= 27))
    .select(
        "*",
        F.date_trunc("WEEK", "opened_date").alias("opened_date_period"),
        F.coalesce("region", F.lit("N/A")).alias("resolved_region"),
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.dense_rank().over(w2).alias("sqft_num_dense_rank"),
        F.sum("sqft").over(w3).alias("cumulative_sqft"),
        F.lag("sqft", 2).over(w4).alias("prev_sqft"),
        F.ntile(4).over(w5).alias("sqft_num_bucket"),
        F.dense_rank().over(w6).alias("sqft_num_dense_rank_2"),
    )
)

filtered_stores = (
    metrics
    .filter(F.col("region").isNotNull())
    .select(
        "*",
        (F.col("sqft_num") - F.col("sqft_num_bucket")).alias("sqft_num_minus_sqft_num_bucket"),
    )
)

eligible = (
    filtered_stores
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.revenue").isNotNull()), "left_anti")
)

metrics_stores = (
    eligible
    .withColumn("prev_sqft_avg_30d", F.avg("prev_sqft").over(w7))
    .withColumn("cumulative_sqft_percent_rank", F.percent_rank().over(w8))
    .withColumn("prev_sqft_num_dense_rank_2", F.lag("sqft_num_dense_rank_2", 1).over(w9))
)

result = (
    metrics_stores
    .select(
        "id",
        "store_name",
        "region",
        "sqft",
        "region_store_name_key",
        "sqft_num",
        "opened_date_period",
        "cumulative_sqft",
        "prev_sqft",
    )
    .orderBy(F.col("opened_date_period").desc())
)
