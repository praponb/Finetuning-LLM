from pyspark.sql import functions as F

valid = (
    df_stores
    .filter((F.col("opened_date") >= F.lit("2024-02-06").cast("date")) & F.col("region").isNotNull())
    .select(
        "*",
        F.when(F.col("region") == "Central", 1).otherwise(0).alias("is_central"),
        F.concat_ws(",", "region", "store_name").alias("region_store_name_key"),
        F.coalesce("region", F.lit("OTHER")).alias("region_clean"),
    )
)

kept = (
    valid
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.sale_date") < F.col("s.opened_date")), "left_semi")
)

not_flagged = (
    kept
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.discount") < 0.21), "left_anti")
)

result = (
    not_flagged
    .groupBy(
        F.when(F.col("is_central") >= 63, "HIGH").otherwise("LOW").alias("is_central_tier"),
    )
    .agg(
        F.round(F.avg("is_central"), 2).alias("avg_is_central"),
        F.size(F.collect_set("region")).alias("num_regions"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.percentile_approx("is_central", 0.9).alias("is_central_p90"),
        F.sort_array(F.collect_set("region")).alias("region_list_2"),
    )
    .orderBy(F.col("is_central_tier").desc())
)
