from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy("opened_date", "id")
w2 = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(-3, -1)
w3 = Window.partitionBy("city").orderBy(F.col("sqft").desc_nulls_last())

base = (
    df_stores
    .filter((F.col("opened_date") > F.lit("2023-08-02").cast("date")) & F.col("region").isNotNull())
    .select(
        "*",
        F.col("sqft").cast("string").alias("sqft_num"),
        F.coalesce("region", F.lit("OTHER")).alias("resolved_region"),
    )
)

eligible = (
    base
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.shipping_fee") > 33), "left_anti")
)

enriched = (
    eligible
    .select(
        "id",
        "store_name",
        "resolved_region",
        F.row_number().over(w).alias("event_seq"),
        F.avg("sqft").over(w2).alias("sqft_avg_3_prior"),
        F.cume_dist().over(w3).alias("sqft_cume_dist"),
    )
)

kept = (
    enriched
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.store_id").isin([3, 5])), "left_anti")
)

result = (
    kept
    .groupBy(
        F.when(F.col("event_seq") >= 7, "LARGE").otherwise("SMALL").alias("event_seq_tier"),
    )
    .agg(
        F.percentile_approx("sqft_avg_3_prior", 0.9).alias("sqft_avg_3_prior_p90"),
        F.percentile_approx("sqft_cume_dist", [0.25, 0.5, 0.75]).alias("sqft_cume_dist_quartiles"),
        F.round(F.avg("sqft_avg_3_prior"), 2).alias("avg_sqft_avg_3_prior"),
    )
)
