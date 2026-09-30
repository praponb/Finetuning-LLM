from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("city", "country").orderBy("rating_filled", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("country").orderBy(F.col("rating_filled").desc(), "id")

valid = (
    df_suppliers
    .filter(F.col("country").isin(["UK", "US", "JP"]))
    .select(
        "*",
        F.concat_ws(",", "city", "supplier_name").alias("city_supplier_name_key"),
        F.upper("supplier_name").alias("supplier_name_upper"),
    )
)

valid_suppliers = (
    valid
    .filter(F.col("country") != "TH")
    .select(
        "*",
        F.coalesce("rating", F.lit(0)).alias("rating_filled"),
        F.round((F.col("rating") * 0.85), 2).alias("rating_adjusted"),
    )
)

valid_suppliers_2 = (
    valid_suppliers
    .filter(F.col("city").isNotNull() & F.col("city").isNotNull())
    .withColumn("greatest_rating_adjusted_rating_filled", F.greatest("rating_adjusted", "rating_filled"))
)

not_flagged = (
    valid_suppliers_2
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.cost").between(200, 650)), "left_anti")
)

enriched = (
    not_flagged
    .filter(~F.col("supplier_name").like("Gl%"))
    .select(
        "*",
        F.max("rating").over(w_ordered).alias("last4_max_rating"),
        F.count("rating").over(w_ordered).alias("last4_count_rating"),
        F.avg("rating").over(w_ordered).alias("last4_avg_rating"),
        F.min("rating").over(w_ordered).alias("last4_min_rating"),
        F.ntile(10).over(w2).alias("rating_filled_bucket"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("last4_count_rating") >= 17, "LARGE").otherwise("SMALL").alias("last4_count_rating_tier"),
        "country",
        F.when(F.col("rating_filled_bucket") >= 8, "PREMIUM").otherwise("STANDARD").alias("rating_filled_bucket_tier"),
    )
    .agg(
        F.percentile_approx("last4_count_rating", 0.9).alias("last4_count_rating_p90"),
        F.countDistinct("city").alias("distinct_cities"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.count_if(F.col("city") == "London").alias("london_events"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
    .filter(F.col("london_events") > 1)
    .orderBy(F.col("london_events").asc_nulls_first())
)
