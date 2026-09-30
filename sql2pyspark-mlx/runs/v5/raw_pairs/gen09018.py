from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("opened_date_plus_months", "id").rowsBetween(-3, -1)
w2 = Window.orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("region").orderBy(F.col("sqft").asc_nulls_last())
w_recent = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(-5, -1)
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w6 = Window.orderBy("cumulative_sqft", "id")
w7 = Window.orderBy("cumulative_sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy("sqft", "id").rowsBetween(-5, Window.currentRow)
w_running = Window.orderBy("cumulative_sqft", "id").rowsBetween(-2, -1)

valid = (
    df_stores
    .filter(F.col("opened_date") > F.lit("2023-06-29").cast("date"))
    .select(
        "*",
        F.add_months("opened_date", 12).alias("opened_date_plus_months"),
        F.coalesce("region", F.lit("NONE")).alias("resolved_region"),
    )
)

enriched = (
    valid
    .filter(F.upper("store_name").like("%IRP%"))
    .select(
        "id",
        "sqft",
        "opened_date_plus_months",
        "resolved_region",
        F.avg("sqft").over(w).alias("sqft_avg_3_prior"),
        F.sum("sqft").over(w2).alias("cumulative_sqft"),
        F.dense_rank().over(w3).alias("sqft_dense_rank"),
        F.avg("sqft").over(w_recent).alias("sqft_avg_5_prior"),
    )
)

latest_match = (
    enriched
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.last_restock") <= F.col("s.opened_date_plus_months")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.last_restock").alias("inventory_last_restock"),
        F.col("c.quantity"),
    )
)

metrics = (
    latest_match
    .select(
        "*",
        F.ntile(3).over(w6).alias("cumulative_sqft_bucket"),
        F.last("sqft_avg_3_prior", ignorenulls=True).over(w7).alias("last_known_sqft_avg_3_prior"),
        F.avg("sqft").over(w_hist).alias("sqft_avg_5"),
        F.avg("sqft").over(w_running).alias("sqft_avg_2_prior"),
    )
)

eligible = (
    metrics
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.product_id") == 3), "left_semi")
)

result = (
    eligible
    .groupBy(F.date_trunc("QUARTER", "inventory_last_restock").alias("quarter_start"))
    .agg(
        F.stddev_samp("sqft").alias("sqft_stddev"),
        F.bit_and("sqft").alias("sqft_bit_and"),
    )
    .orderBy(F.col("sqft_bit_and").desc_nulls_first())
)
