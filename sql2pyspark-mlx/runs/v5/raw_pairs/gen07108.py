from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("region").orderBy("opened_date", "id")
w2 = Window.partitionBy("region", "city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("region", "city").orderBy("store_sale_units", "id")
w5 = Window.partitionBy("region")
w6 = Window.partitionBy("region").orderBy("latest_rank", "id")
w7 = Window.partitionBy("city").orderBy(F.col("store_sale_units").asc_nulls_first())
w8 = Window.partitionBy("city").orderBy("store_sale_revenue", "id")
w9 = Window.partitionBy("region").orderBy("store_sale_units_cume_dist", "id").rowsBetween(-5, -1)

filtered = (
    df_stores
    .filter((F.col("opened_date") >= F.to_date(F.lit("2023-04-24"))) & (F.col("region") == "Central"))
    .withColumn("region_prefix", F.substring("region", 1, 2))
)

scored = (
    filtered
    .withColumn("next_sqft", F.lead("sqft").over(w_running))
    .withColumn("prev_sqft", F.lag("sqft").over(w_running))
    .withColumn("event_seq", F.row_number().over(w_running))
    .withColumn("cumulative_sqft", F.sum("sqft").over(w2))
)

latest_match = (
    scored
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.sale_date") <= F.col("s.opened_date")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.city",
        "s.region",
        "s.region_prefix",
        "s.prev_sqft",
        F.col("c.units").alias("store_sale_units"),
        F.col("c.revenue").alias("store_sale_revenue"),
    )
)

deduped = (
    latest_match
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
)

scored_stores = (
    deduped
    .select(
        "*",
        F.round((F.col("store_sale_revenue") / F.sum("store_sale_revenue").over(w5)), 4).alias("store_sale_revenue_share"),
        F.row_number().over(w6).alias("event_seq"),
        F.cume_dist().over(w7).alias("store_sale_units_cume_dist"),
        F.max("prev_sqft").over(w5).alias("region_max_prev_sqft"),
    )
)

not_flagged = (
    scored_stores
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.store_id") == 2), "left_anti")
)

result = (
    not_flagged
    .select(
        "id",
        "city",
        "region",
        "region_prefix",
        "store_sale_units",
        "store_sale_units_cume_dist",
        "region_max_prev_sqft",
        F.ntile(10).over(w8).alias("store_sale_revenue_bucket"),
        F.avg("store_sale_units").over(w9).alias("store_sale_units_avg_5_prior"),
    )
)
