from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("sqft").asc_nulls_first(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())
w3 = Window.orderBy(F.col("store_sale_revenue").desc())
w4 = Window.partitionBy("region", "city").orderBy(F.col("store_sale_revenue").desc_nulls_first())
w5 = Window.orderBy(F.col("store_sale_revenue").asc_nulls_first())
w_running = Window.orderBy("store_sale_revenue_dense_rank")
w7 = Window.orderBy(F.col("store_sale_revenue_rank").desc())
w8 = Window.orderBy("store_sale_revenue_dense_rank_2")
w_part = Window.orderBy(F.col("store_sale_revenue_dense_rank_2").desc())

filtered = (
    df_stores
    .filter(F.lower(F.trim("store_name")) == "riverside")
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft",
        F.concat_ws("/", "region", "store_name").alias("region_store_name_key"),
    )
)

latest = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "store_name", "city", "region")
)

with_last = (
    latest
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.revenue").alias("store_sale_revenue"),
        F.col("c.sale_date").alias("store_sale_sale_date"),
    )
)

matched_keys = (
    with_last
    .select("id", "region")
    .subtract(
        df_stores
        .filter(F.col("opened_date").between("2023-10-17", "2025-07-11"))
        .select("id", "region")
    )
)

verified = (
    with_last
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.region") == F.col("k.region")), "inner")
    .select("s.*")
)

scored = (
    verified
    .select(
        "id",
        "store_name",
        F.rank().over(w3).alias("store_sale_revenue_rank"),
        F.dense_rank().over(w4).alias("store_sale_revenue_dense_rank"),
        F.dense_rank().over(w5).alias("store_sale_revenue_dense_rank_2"),
    )
)

scored_stores = (
    scored
    .filter(F.col("store_sale_revenue_dense_rank_2").between(5, 6))
    .select(
        "store_sale_revenue_rank",
        "store_sale_revenue_dense_rank_2",
        F.dense_rank().over(w_running).alias("store_sale_revenue_dense_rank_dense_rank"),
        F.dense_rank().over(w7).alias("store_sale_revenue_rank_dense_rank"),
        F.dense_rank().over(w8).alias("store_sale_revenue_dense_rank_2_dense_rank"),
        F.rank().over(w_part).alias("store_sale_revenue_dense_rank_2_rank"),
    )
)

with_windows = (
    scored_stores
    .select(
        "store_sale_revenue_dense_rank_2",
        "store_sale_revenue_rank_dense_rank",
        F.dense_rank().over(w_part).alias("store_sale_revenue_dense_rank_2_dense_rank_2"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("store_sale_revenue_rank_dense_rank") >= 9, "PREMIUM").otherwise("STANDARD").alias("store_sale_revenue_rank_dense_rank_tier"),
    )
    .agg(
        F.min("store_sale_revenue_dense_rank_2").alias("min_store_sale_revenue_dense_rank_2"),
        F.percentile_approx("store_sale_revenue_dense_rank_2", [0.25, 0.5, 0.75]).alias("store_sale_revenue_dense_rank_2_quartiles"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("store_sale_revenue_rank_dense_rank_tier").desc())
)
