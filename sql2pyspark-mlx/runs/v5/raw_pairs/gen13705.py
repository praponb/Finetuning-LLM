from pyspark.sql import functions as F

kept_keys = (
    df_shipments
    .select("order_id", "id")
    .intersectAll(
        df_shipments
        .filter(F.month("delivered_date").isin([5, 8, 9]))
        .select("order_id", "id")
    )
)

kept_rows = (
    df_shipments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(F.date_trunc("quarter", "shipped_date").alias("quarter_start"))
    .agg(
        F.sum(F.when(F.col("carrier") == "Kerry", F.col("cost")).otherwise(0)).alias("kerry_cost"),
        F.count(F.when(F.col("status") == "IN_TRANSIT", 1)).alias("in_transit_rows"),
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
        F.stddev_samp("cost").alias("cost_stddev"),
        F.round(F.sum("cost") / F.nullif(F.count(F.when(F.col("status") == "DELIVERED", 1)), F.lit(0)), 2).alias("cost_per_delivered"),
    )
)
