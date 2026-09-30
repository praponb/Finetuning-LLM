from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("sqft").asc_nulls_last())
w2 = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("sqft", "id")
w_grp = Window.partitionBy("city", "region").orderBy("sqft", "id")
w5 = Window.partitionBy("city").orderBy("sqft_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("region").orderBy("prev_sqft", "id").rowsBetween(-5, -1)
w7 = Window.partitionBy("region").orderBy(F.col("last_known_prev_sqft").desc_nulls_first())
w8 = Window.partitionBy("city").orderBy("prev_sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("region").orderBy(F.col("prev_sqft").desc(), "id")
w_recent = Window.orderBy("sqft_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("region").orderBy(F.col("prev_sqft_percent_rank").asc_nulls_last())
w12 = Window.partitionBy("city", "region").orderBy("last_known_prev_sqft_rank", "id")
w13 = Window.partitionBy("region").orderBy("sqft_avg_5_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_stores
    .filter((F.col("opened_date") > F.lit("2023-04-15").cast("date")) & F.month("opened_date").isin([2, 6, 11]))
    .select("*", F.coalesce("store_name", F.lit("N/A")).alias("store_name_filled"))
)

metrics = (
    base
    .select(
        "*",
        F.percent_rank().over(w).alias("sqft_percent_rank"),
        F.first("region", ignorenulls=True).over(w2).alias("first_known_region"),
        F.lag("sqft", 2).over(w3).alias("prev_sqft"),
    )
)

metrics_stores = (
    metrics
    .withColumn("prev_sqft_percent_rank", F.lag("sqft_percent_rank", 1).over(w_grp))
    .withColumn("last_known_prev_sqft", F.last("prev_sqft", ignorenulls=True).over(w5))
    .withColumn("sqft_avg_5_prior", F.avg("sqft").over(w6))
)

metrics_stores_2 = (
    metrics_stores
    .select(
        "*",
        F.lag("sqft_avg_5_prior", 1).over(w_grp).alias("prev_sqft_avg_5_prior"),
        F.rank().over(w7).alias("last_known_prev_sqft_rank"),
        F.last("sqft_percent_rank", ignorenulls=True).over(w8).alias("last_known_sqft_percent_rank"),
    )
)

enriched = (
    metrics_stores_2
    .select(
        "*",
        F.ntile(10).over(w9).alias("prev_sqft_bucket"),
        F.last("sqft", ignorenulls=True).over(w_recent).alias("last_known_sqft"),
        F.percent_rank().over(w11).alias("prev_sqft_percent_rank_percent_rank"),
        F.ntile(10).over(w12).alias("last_known_prev_sqft_rank_bucket"),
        F.sum("sqft").over(w13).alias("cumulative_sqft"),
    )
)

eligible = (
    enriched
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.sale_date") >= F.col("s.opened_date")), "left_anti")
)

totals = (
    eligible
    .withColumn("year_start", F.date_trunc("YEAR", "opened_date"))
    .withColumn("activity_month", F.date_trunc("MONTH", "opened_date"))
    .groupingSets(
        [["year_start", "activity_month"], ["year_start"], []],
        "year_start",
        "activity_month",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("city") == "Bangkok", 1)).alias("bangkok_count"),
        F.max(F.when(F.col("region") == "North", F.col("last_known_sqft"))).alias("north_max_last_known_sqft"),
    )
)

result = (
    totals
    .select(
        "year_start",
        "activity_month",
        "cnt",
        "bangkok_count",
        "north_max_last_known_sqft",
    )
)
