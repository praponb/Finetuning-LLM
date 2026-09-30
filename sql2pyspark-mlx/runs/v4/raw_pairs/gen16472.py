from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())
w_running = Window.partitionBy("country").orderBy(F.col("rating").desc(), "id")
w3 = Window.partitionBy("country", "city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.partitionBy("country")
w6 = Window.orderBy("product_launch_date", "id")

base = (
    df_suppliers
    .filter(F.col("city").isNotNull() & (F.col("rating") > 3.8))
    .withColumn("country_lower", F.lower("country"))
    .withColumn("city_filled", F.coalesce("city", F.lit("NONE")))
)

latest_match = (
    base
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.launch_date").alias("product_launch_date"))
)

metrics = (
    latest_match
    .withColumn("seq_no", F.row_number().over(w_running))
    .withColumn("running_rating", F.sum("rating").over(w3))
    .withColumn("rating_avg_3", F.avg("rating").over(w_part))
)

not_flagged = (
    metrics
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.launch_date") >= F.col("s.product_launch_date")), "left_anti")
)

eligible = (
    not_flagged
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.launch_date") < F.col("s.product_launch_date")), "left_anti")
)

metrics_suppliers = (
    eligible
    .withColumn("country_stddev_samp_seq_no", F.stddev_samp("seq_no").over(w5))
    .withColumn("next_seq_no", F.lead("seq_no", 1, 0).over(w6))
)

grouped = (
    metrics_suppliers
    .withColumn("next_seq_no_tier", F.when(F.col("next_seq_no") >= 7, "HIGH").otherwise("LOW"))
    .withColumn("week_start", F.date_trunc("WEEK", "product_launch_date"))
    .cube("next_seq_no_tier", "country", "week_start")
    .agg(
        F.size(F.collect_set("city")).alias("num_cities"),
        F.round(F.avg("running_rating"), 2).alias("avg_running_rating"),
        F.size(F.collect_set("city")).alias("num_cities_2"),
        F.sum(F.when(F.col("city") == "Chiang Mai", F.col("next_seq_no"))).alias("chiang_mai_total_next_seq_no"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.round(F.avg("rating_avg_3"), 2).alias("avg_rating_avg_3"),
        F.grouping_id().alias("gid"),
    )
    .filter(F.col("num_cities_2") >= 1)
)

result = (
    grouped
    .select(
        "next_seq_no_tier",
        "country",
        "week_start",
        "avg_running_rating",
        "num_cities_2",
        "city_list",
        "gid",
    )
)
