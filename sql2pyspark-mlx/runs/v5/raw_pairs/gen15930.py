from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("region_length").desc_nulls_last())
w2 = Window.partitionBy("region").orderBy("latest_rank", "id")
w3 = Window.partitionBy("region").orderBy(F.col("rn").desc())

cleaned = (
    df_stores
    .filter((F.col("opened_date") >= F.lit("2023-09-17").cast("date")) & (F.col("region").eqNullSafe(F.lit("Central"))))
    .select(
        "id",
        "region",
        F.length("region").alias("region_length"),
        F.substring("region", 1, 3).alias("region_prefix"),
    )
)

eligible = (
    cleaned
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.month(F.col("ch.sale_date")).isin([2, 4, 5])), "left_semi")
)

deduped = (
    eligible
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

deduped_stores = (
    deduped
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

kept = (
    deduped_stores
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.order_date") <= "2023-09-01"), "left_semi")
)

latest = (
    kept
    .withColumn("latest_rank_2", F.rank().over(w3))
    .filter(F.col("latest_rank_2") == 1)
)

daily = (
    latest
    .groupBy(
        F.when(F.col("region_length") >= 73, "HIGH").otherwise("LOW").alias("region_length_tier"),
    )
    .agg(
        F.sum(F.when(F.col("region") == "North", F.col("region_length")).otherwise(0)).alias("north_region_length"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
    )
)

result = daily.select("region_length_tier", "distinct_ids", "region_list")
