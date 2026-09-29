from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy(F.col("opened_date").asc_nulls_last(), "id")
w_grp = Window.partitionBy("region").orderBy("opened_date", "id")
w3 = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(-6, Window.currentRow)
w5 = Window.partitionBy("city").orderBy(F.col("opened_date").desc(), "id")
w_running = Window.orderBy("rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_stores
    .filter((F.col("opened_date") > F.lit("2023-05-30").cast("date")) & F.col("region").isNotNull())
    .withColumn("city_upper", F.upper("city"))
    .withColumn("region_filled", F.coalesce("region", F.lit("DIRECT")))
)

not_flagged = (
    cleaned
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.sale_date") < F.col("s.opened_date")), "left_anti")
)

ranked = (
    not_flagged
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

scored = (
    ranked
    .filter(F.col("region").isNotNull())
    .select(
        "*",
        F.lead("sqft", 1).over(w_grp).alias("next_sqft"),
        F.sum("sqft").over(w3).alias("running_sqft"),
        F.avg("sqft").over(w4).alias("sqft_avg_6"),
        F.row_number().over(w5).alias("event_seq"),
        F.sum("sqft").over(w_running).alias("cumulative_sqft"),
    )
)

result = (
    scored
    .groupBy("region")
    .pivot("city", ["Bangkok", "Chiang Mai", "London"])
    .agg(F.avg("event_seq").alias("value_sum"))
    .select(
        "region",
        F.col("Bangkok").alias("bangkok"),
        F.col("Chiang Mai").alias("chiang_mai"),
        F.col("London").alias("london"),
    )
)
