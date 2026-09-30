from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").asc_nulls_last(), "id")
w2 = Window.partitionBy("country", "city").orderBy("rn", "id")
w3 = Window.partitionBy("city", "country")
w4 = Window.partitionBy("country").orderBy(F.col("rating").desc(), "id")
w_ordered = Window.partitionBy("country")
w6 = Window.partitionBy("country").orderBy("prev_rating", "id").rowsBetween(-6, Window.currentRow)
w7 = Window.partitionBy("city").orderBy(F.col("rating_share").asc_nulls_last())
w8 = Window.partitionBy("city").orderBy("rating_share", "id")
w9 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w10 = Window.partitionBy("city").orderBy("rating", "id")
w11 = Window.partitionBy("city").orderBy("rating_share_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.partitionBy("city").orderBy("rating_share", "id").rowsBetween(-3, -1)
w13 = Window.orderBy(F.col("prev_rating").asc_nulls_last())
w14 = Window.orderBy("prev_rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w15 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())

ranked = (
    df_suppliers
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

metrics = (
    ranked
    .select(
        "*",
        F.lag("rating", 1, 0).over(w2).alias("prev_rating"),
        F.round((F.col("rating") / F.sum("rating").over(w3)), 4).alias("rating_share"),
    )
)

scored = (
    metrics
    .withColumn("rating_bucket", F.ntile(3).over(w4))
    .withColumn("country_count_prev_rating", F.count("prev_rating").over(w_ordered))
    .withColumn("prev_rating_avg_6", F.avg("prev_rating").over(w6))
    .withColumn("rating_share_cume_dist", F.cume_dist().over(w7))
    .withColumn("rating_share_bucket", F.ntile(5).over(w8))
)

with_latest = (
    scored
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn_2", F.row_number().over(w9))
    .filter(F.col("rn_2") == 1)
    .select("s.*", F.col("c.stock_qty").alias("product_stock_qty"))
)

base = (
    with_latest
    .filter(F.col("country") == "US")
    .select(
        "*",
        (F.col("rating") - F.col("product_stock_qty")).alias("rating_minus_product_stock_qty"),
        F.coalesce("city", F.lit("N/A")).alias("city_filled"),
    )
)

enriched = (
    base
    .select(
        "*",
        F.lead("prev_rating_avg_6", 2).over(w10).alias("next_prev_rating_avg_6"),
        F.first("rating_share_bucket", ignorenulls=True).over(w11).alias("first_known_rating_share_bucket"),
        F.avg("country_count_prev_rating").over(w12).alias("country_count_prev_rating_avg_3_prior"),
        F.percent_rank().over(w13).alias("prev_rating_percent_rank"),
        F.sum("country_count_prev_rating").over(w14).alias("running_country_count_prev_rating"),
    )
)

latest_match = (
    enriched
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn_2", F.row_number().over(w15))
    .filter(F.col("rn_2") == 1)
    .select("s.*", F.col("c.cost"), F.col("c.stock_qty"))
)

result = (
    latest_match
    .withColumn("cost_tier", F.when(F.col("cost") >= 750, "HIGH").otherwise("LOW"))
    .withColumn("prev_rating_percent_rank_tier", F.when(F.col("prev_rating_percent_rank") >= 0.86, "LARGE").otherwise("SMALL"))
    .rollup("cost_tier", "country", "prev_rating_percent_rank_tier")
    .agg(
        F.max(F.when(F.col("country") == "DE", F.col("country_count_prev_rating_avg_3_prior"))).alias("de_max_country_count_prev_rating_avg_3_prior"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy("prev_rating_percent_rank_tier")
)
