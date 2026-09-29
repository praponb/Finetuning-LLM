from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating_scaled").asc_nulls_first())
w2 = Window.partitionBy("city").orderBy("rank_in_group")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())
w_ordered = Window.partitionBy("city").orderBy("product_launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_suppliers
    .filter(~F.col("city").eqNullSafe(F.lit("Bangkok")))
    .select(
        "id",
        "supplier_name",
        "city",
        F.round((F.col("rating") * 0.85), 2).alias("rating_scaled"),
    )
)

deduped = (
    cleaned
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

deduped_suppliers = (
    deduped
    .withColumn("latest_rank", F.rank().over(w2))
    .filter(F.col("latest_rank") == 1)
)

eligible = (
    deduped_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.brand").isin(["Initech", "Umbrella"])), "left_semi")
)

latest_match = (
    eligible
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.launch_date").alias("product_launch_date"))
)

metrics = (
    latest_match
    .select(
        "id",
        "supplier_name",
        F.avg("rating_scaled").over(w_ordered).alias("running_avg_rating_scaled"),
        F.min("rating_scaled").over(w_ordered).alias("running_min_rating_scaled"),
        F.max("rating_scaled").over(w_ordered).alias("running_max_rating_scaled"),
    )
)

stats = (
    metrics
    .withColumn("running_max_rating_scaled_tier", F.when(F.col("running_max_rating_scaled") >= 82, "LARGE").otherwise("SMALL"))
    .rollup("running_max_rating_scaled_tier")
    .agg(
        F.percentile_approx("running_min_rating_scaled", [0.25, 0.5, 0.75]).alias("running_min_rating_scaled_quartiles"),
        F.percentile_approx("running_avg_rating_scaled", [0.25, 0.5, 0.75]).alias("running_avg_rating_scaled_quartiles"),
        F.round(F.avg("running_avg_rating_scaled"), 2).alias("avg_running_avg_rating_scaled"),
        F.grouping("running_max_rating_scaled_tier").alias("grouping_id"),
    )
    .filter(F.col("avg_running_avg_rating_scaled") > 0)
)

result = (
    stats
    .select(
        "running_avg_rating_scaled_quartiles",
        "avg_running_avg_rating_scaled",
        "grouping_id",
    )
)
