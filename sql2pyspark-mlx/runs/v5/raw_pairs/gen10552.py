from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("rating", "id")
w2 = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_suppliers
    .filter(F.col("rating").between(3.0, 3.3))
    .select(
        "id",
        "supplier_name",
        "rating",
        F.concat("supplier_name", F.lit(" - "), "city").alias("supplier_name_city"),
        F.when(F.col("rating") >= 4.0, "High").when(F.col("rating") >= 1.7, "Medium").otherwise("Low").alias("rating_tier"),
    )
)

with_windows = (
    base
    .select(
        "id",
        "rating",
        "rating_tier",
        F.ntile(3).over(w).alias("rating_bucket"),
        F.sum("rating").over(w2).alias("cumulative_rating"),
    )
)

kept = (
    with_windows
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.category") == "Books"), "left_semi")
)

scored = (
    kept
    .filter(F.col("rating") < 1.9)
    .select(
        "id",
        "rating_tier",
        F.min("cumulative_rating").over(w2).alias("running_min_cumulative_rating"),
        F.max("cumulative_rating").over(w2).alias("running_max_cumulative_rating"),
        F.sum("cumulative_rating").over(w2).alias("running_sum_cumulative_rating"),
    )
)

eligible = (
    scored
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.price") > 1100), "left_anti")
)

result = (
    eligible
    .withColumn("running_min_cumulative_rating_tier", F.when(F.col("running_min_cumulative_rating") >= 18.6, "HIGH").otherwise("LOW"))
    .cube("running_min_cumulative_rating_tier")
    .agg(
        F.round(F.avg("running_min_cumulative_rating"), 2).alias("avg_running_min_cumulative_rating"),
        F.countDistinct("id").alias("distinct_ids"),
        F.max("running_min_cumulative_rating").alias("max_running_min_cumulative_rating"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.round(F.avg("running_sum_cumulative_rating"), 2).alias("avg_running_sum_cumulative_rating"),
        F.grouping_id().alias("gid"),
    )
    .filter(F.col("distinct_ids") < 1000)
    .orderBy(F.col("max_running_min_cumulative_rating").asc_nulls_first())
)
