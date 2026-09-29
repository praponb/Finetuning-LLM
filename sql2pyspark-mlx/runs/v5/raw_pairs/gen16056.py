from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("country", "city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("country").orderBy(F.col("product_cost").asc_nulls_first(), "id")
w4 = Window.partitionBy("country").orderBy(F.col("product_cost").desc_nulls_first(), "id")

enriched = (
    df_suppliers
    .filter(F.col("supplier_name").like("Pa%"))
    .withColumn("running_sum_rating", F.sum("rating").over(w_recent))
    .withColumn("running_max_rating", F.max("rating").over(w_recent))
    .withColumn("running_min_rating", F.min("rating").over(w_recent))
)

with_last = (
    enriched
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.country",
        "s.running_max_rating",
        F.col("c.launch_date").alias("product_launch_date"),
        F.col("c.cost").alias("product_cost"),
    )
)

ranked = (
    with_last
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

latest = (
    ranked
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .groupBy(
        F.date_trunc("WEEK", "product_launch_date").alias("week_start"),
        F.date_trunc("YEAR", "product_launch_date").alias("year_start"),
    )
    .agg(
        F.countDistinct("country").alias("distinct_countries"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.percentile_approx("product_cost", 0.9).alias("product_cost_p90"),
        F.percentile_approx("running_max_rating", 0.9).alias("running_max_rating_p90"),
    )
    .orderBy("running_max_rating_p90")
)
