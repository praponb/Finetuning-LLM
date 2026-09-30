from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region", "store_id").orderBy(F.col("order_date_month_end").asc_nulls_first(), "id")
w2 = Window.partitionBy("region").orderBy("order_date_month_end", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("region")
w4 = Window.partitionBy("region").orderBy("dedup_rank_adjusted", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_orders
    .filter((F.col("order_date") > F.lit("2023-07-03").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "store_id",
        "status",
        "amount",
        "shipping_fee",
        F.last_day("order_date").alias("order_date_month_end"),
        F.coalesce("channel", F.lit("OTHER")).alias("channel_filled"),
    )
)

enriched = (
    valid
    .alias("src")
    .join(df_stores.alias("p"), F.col("src.store_id") == F.col("p.id"), "left")
    .select("src.id", "src.store_id", "src.order_date_month_end", F.col("p.region"))
)

ranked = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

valid_orders = (
    ranked
    .filter(F.year("order_date_month_end") >= 2025)
    .select(
        "id",
        "order_date_month_end",
        "region",
        F.round((F.col("dedup_rank") * 0.85), 2).alias("dedup_rank_adjusted"),
        F.coalesce("region", F.lit("N/A")).alias("region_clean"),
    )
)

scored = (
    valid_orders
    .filter(F.col("dedup_rank_adjusted") < 43)
    .withColumn("dedup_rank_adjusted_avg_5_prior", F.avg("dedup_rank_adjusted").over(w2))
    .withColumn("region_sum_dedup_rank_adjusted", F.sum("dedup_rank_adjusted").over(w3))
    .withColumn("running_dedup_rank_adjusted", F.sum("dedup_rank_adjusted").over(w4))
)

result = (
    scored
    .groupBy(
        F.when(F.col("running_dedup_rank_adjusted") >= 338, "LARGE").otherwise("SMALL").alias("running_dedup_rank_adjusted_tier"),
    )
    .agg(
        F.round(F.sum("running_dedup_rank_adjusted") / F.nullif(F.count(F.when(F.col("region") == "South", 1)), F.lit(0)), 2).alias("running_dedup_rank_adjusted_per_south"),
        F.avg(F.when(F.col("region") == "South", F.col("dedup_rank_adjusted"))).alias("south_avg_dedup_rank_adjusted"),
        F.count_if(F.col("region") == "Central").alias("central_events"),
    )
    .filter(F.col("south_avg_dedup_rank_adjusted") > 0)
    .orderBy(F.col("running_dedup_rank_adjusted_per_south").desc())
)
