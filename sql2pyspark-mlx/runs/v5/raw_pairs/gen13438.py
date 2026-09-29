from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("opened_date").desc(), "id")
w2 = Window.partitionBy("city").orderBy(F.col("sqft").desc(), "id")
w3 = Window.partitionBy("region").orderBy(F.col("sqft").desc_nulls_first())
w4 = Window.partitionBy("region").orderBy("sqft", "id")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())

latest = (
    df_stores
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

metrics = (
    latest
    .withColumn("sqft_bucket", F.ntile(5).over(w2))
    .withColumn("sqft_dense_rank", F.dense_rank().over(w3))
    .withColumn("sqft_bucket_2", F.ntile(4).over(w4))
)

with_latest = (
    metrics
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_name",
        "s.region",
        "s.opened_date",
        "s.sqft_bucket",
        "s.sqft_dense_rank",
        F.col("c.quantity"),
    )
)

with_latest_stores = (
    with_latest
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.opened_date",
        "s.sqft_dense_rank",
        F.col("c.units").alias("store_sale_units"),
    )
)

eligible = (
    with_latest_stores
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.sale_date") <= F.col("s.opened_date")), "left_semi")
)

cleaned = (
    eligible
    .filter(F.col("store_sale_units") > 38)
    .select("*", F.year("opened_date").alias("opened_date_year"))
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("opened_date_year") >= 64, "LARGE").otherwise("SMALL").alias("opened_date_year_tier"),
    )
    .agg(
        F.round(F.avg("opened_date_year"), 2).alias("avg_opened_date_year"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("opened_date_year", 0.9).alias("opened_date_year_p90"),
        F.percentile_approx("sqft_dense_rank", [0.25, 0.5, 0.75]).alias("sqft_dense_rank_quartiles"),
        F.min("opened_date_year").alias("min_opened_date_year"),
    )
    .orderBy("avg_opened_date_year")
)
