from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("cost_int", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id")
w3 = Window.partitionBy("order_id", "carrier")
w_recent = Window.orderBy(F.col("cost_int").asc_nulls_last())

filtered = (
    df_shipments
    .filter(F.col("order_id").isNotNull() & (F.year("delivered_date") >= 2025))
    .select(
        "*",
        F.trim("carrier").alias("clean_carrier"),
        F.col("cost").cast("int").alias("cost_int"),
    )
)

enriched = (
    filtered
    .filter(F.col("status").isin(["RETURNED", "LOST", "IN_TRANSIT"]))
    .select(
        "id",
        "cost",
        "cost_int",
        F.last("carrier", ignorenulls=True).over(w).alias("last_known_carrier"),
        F.round((F.col("cost") / F.sum("cost").over(w2)), 4).alias("cost_share"),
        F.round((F.col("cost") / F.sum("cost").over(w3)), 4).alias("cost_share_2"),
    )
)

kept_keys = (
    enriched
    .select("id")
    .intersect(
        df_shipments
        .filter(F.col("status") != "DELIVERED")
        .select("id")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched_shipments = kept_rows.select("*", F.dense_rank().over(w_recent).alias("cost_int_dense_rank"))

result = (
    enriched_shipments
    .withColumn("cost_share_2_tier", F.when(F.col("cost_share_2") >= 0.83, "PREMIUM").otherwise("STANDARD"))
    .withColumn("cost_share_tier", F.when(F.col("cost_share") >= 0.78, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["cost_share_2_tier", "cost_share_tier"], ["cost_share_2_tier"], []],
        "cost_share_2_tier",
        "cost_share_tier",
    )
    .agg(
        F.percentile_approx("cost_share", [0.25, 0.5, 0.75]).alias("cost_share_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("cost_int_dense_rank"), 2).alias("avg_cost_int_dense_rank"),
        F.percentile_approx("cost_share", 0.5).alias("cost_share_p50"),
        F.percentile_approx("cost_int", 0.5).alias("cost_int_p50"),
        F.percentile_approx("cost_int", [0.25, 0.5, 0.75]).alias("cost_int_quartiles"),
    )
    .filter(F.col("cost_share_p50") > 0)
    .orderBy(F.col("distinct_ids").asc_nulls_first())
)
