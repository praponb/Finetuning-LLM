from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.orderBy(F.col("sqft").desc(), "id")
w3 = Window.partitionBy("region").orderBy("opened_date", "id")
w4 = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w6 = Window.orderBy("inventory_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy(F.col("inventory_quantity").desc_nulls_last())
w8 = Window.orderBy("next_sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("sqft", "id").rowsBetween(-5, -1)
w10 = Window.orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_stores
    .filter(F.length("store_name") > 9)
    .select(
        "id",
        "store_name",
        "sqft",
        F.avg("sqft").over(w).alias("sqft_avg_2"),
        F.row_number().over(w2).alias("event_seq"),
        F.lead("sqft", 1).over(w3).alias("next_sqft"),
        F.sum("sqft").over(w4).alias("cumulative_sqft"),
    )
)

with_latest = (
    enriched
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity").alias("inventory_quantity"))
)

scored = (
    with_latest
    .select(
        "id",
        "sqft",
        "sqft_avg_2",
        "inventory_quantity",
        F.sum("event_seq").over(w6).alias("running_event_seq"),
        F.cume_dist().over(w7).alias("inventory_quantity_cume_dist"),
        F.first("sqft_avg_2", ignorenulls=True).over(w8).alias("first_known_sqft_avg_2"),
        F.avg("event_seq").over(w9).alias("event_seq_avg_5_prior"),
        F.sum("inventory_quantity").over(w10).alias("cumulative_inventory_quantity"),
    )
)

eligible = (
    scored
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.customer_id").isNotNull()), "left_anti")
)

kept = (
    eligible
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.store_id").isin([2, 3, 4, 5])), "left_anti")
)

result = (
    kept
    .groupBy(
        F.when(F.col("event_seq_avg_5_prior") >= 7, "PREMIUM").otherwise("STANDARD").alias("event_seq_avg_5_prior_tier"),
    )
    .agg(
        F.percentile_approx("running_event_seq", 0.5).alias("running_event_seq_p50"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("event_seq_avg_5_prior_tier").asc_nulls_last())
)
