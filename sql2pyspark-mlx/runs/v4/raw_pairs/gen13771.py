from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("store_id").orderBy("quantity", "id")
w2 = Window.partitionBy("product_id", "store_id").orderBy(F.col("last_restock").desc(), "id")
w_part_2 = Window.partitionBy("store_id").orderBy(F.col("last_restock_period").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w4 = Window.orderBy("last_restock", "id")
w5 = Window.partitionBy("product_id")
w6 = Window.partitionBy("store_id")
w7 = Window.partitionBy("city").orderBy(F.col("last_restock_period").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w8 = Window.partitionBy("city")
w9 = Window.orderBy(F.col("quantity_count_7d").desc_nulls_last())
w10 = Window.orderBy(F.col("last_restock_period").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w_ordered = Window.partitionBy("product_id").orderBy(F.col("quantity_count_7d").asc_nulls_first())

valid = (
    df_inventory
    .filter((F.col("last_restock") > F.lit("2024-01-31").cast("date")) & F.col("product_id").isin([2, 5, 10]))
    .select(
        "*",
        F.date_trunc("week", "last_restock").alias("last_restock_period"),
        F.last_day("last_restock").alias("last_restock_month_end"),
    )
)

scored = (
    valid
    .withColumn("seq_no", F.row_number().over(w_part))
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("quantity_count_7d", F.count("quantity").over(w_part_2))
    .withColumn("seq_no_2", F.row_number().over(w4))
)

joined = (
    scored
    .alias("a")
    .join(df_stores.alias("s"), F.col("a.store_id") == F.col("s.id"), "left")
    .select("a.*", F.col("s.city"), F.col("s.store_name"), F.col("s.sqft"))
)

remaining_keys = (
    joined
    .select("product_id", "id")
    .intersectAll(
        df_inventory
        .filter(F.col("store_id").isin([1, 4, 5, 6]))
        .select("product_id", "id")
    )
)

kept_rows = (
    joined
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    kept_rows
    .withColumn("seq_no_2_share", F.round((F.col("seq_no_2") / F.sum("seq_no_2").over(w5)), 4))
    .withColumn("store_id_max_seq_no", F.max("seq_no").over(w6))
    .withColumn("quantity_sum_30d", F.sum("quantity").over(w7))
    .withColumn("city_min_sqft", F.min("sqft").over(w8))
)

result = (
    metrics
    .withColumn("city_avg_event_seq", F.avg("event_seq").over(w8))
    .withColumn("quantity_count_7d_cume_dist", F.cume_dist().over(w9))
    .withColumn("seq_no_2_avg_90d", F.avg("seq_no_2").over(w10))
    .withColumn("quantity_count_7d_rank", F.rank().over(w_ordered))
)
