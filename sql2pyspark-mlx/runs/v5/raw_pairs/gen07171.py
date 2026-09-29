from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region")
w2 = Window.orderBy(F.col("sqft").asc_nulls_first())
w3 = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(-3, -1)
w4 = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(-5, Window.currentRow)
w5 = Window.partitionBy("city", "region").orderBy(F.col("opened_date").desc(), "id")
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.order_date").desc(), F.col("c.id").desc())
w7 = Window.partitionBy("row_seq_tier").orderBy(F.col("south_total_sqft_whole_avg_5").desc())

filtered = (
    df_stores
    .filter((F.col("opened_date") >= F.lit("2023-04-16").cast("date")) & F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.ceil("sqft").alias("sqft_whole"),
        F.initcap(F.trim("city")).alias("city_title"),
        F.coalesce("region", F.lit("DIRECT")).alias("resolved_region"),
    )
)

metrics = (
    filtered
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft_whole",
        F.round((F.col("sqft") / F.sum("sqft").over(w)), 4).alias("sqft_share"),
        F.cume_dist().over(w2).alias("sqft_cume_dist"),
        F.avg("sqft").over(w3).alias("sqft_avg_3_prior"),
        F.avg("sqft_whole").over(w4).alias("sqft_whole_avg_5"),
        F.row_number().over(w5).alias("row_seq"),
    )
)

not_flagged = (
    metrics
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.revenue").isNotNull()), "left_anti")
)

not_flagged_stores = (
    not_flagged
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.sale_date") <= "2025-01-01"), "left_semi")
)

with_latest = (
    not_flagged_stores
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.region",
        "s.sqft_whole",
        "s.sqft_share",
        "s.sqft_cume_dist",
        "s.sqft_avg_3_prior",
        "s.sqft_whole_avg_5",
        "s.row_seq",
        F.col("c.amount"),
    )
)

result = (
    with_latest
    .groupBy(
        F.when(F.col("row_seq") >= 8, "PREMIUM").otherwise("STANDARD").alias("row_seq_tier"),
        "region",
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.when(F.col("region") == "South", F.col("sqft_whole_avg_5"))).alias("south_total_sqft_whole_avg_5"),
        F.size(F.collect_set("region")).alias("num_regions"),
    )
    .filter(F.col("south_total_sqft_whole_avg_5") > 0)
    .withColumn("south_total_sqft_whole_avg_5_percent_rank", F.percent_rank().over(w7))
)
