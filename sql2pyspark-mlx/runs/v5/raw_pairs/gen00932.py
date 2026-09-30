from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("rating").desc())
w2 = Window.partitionBy("city", "country")
w3 = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("city").orderBy(F.col("rating_share").desc(), "id")

valid = (
    df_suppliers
    .filter(F.col("city").isNotNull() & F.col("supplier_name").like("%Sia%"))
    .select("*", F.concat("country", F.lit(", "), "city").alias("country_city"))
)

not_flagged = (
    valid
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.launch_date").between("2024-06-26", "2025-06-01")), "left_anti")
)

metrics = (
    not_flagged
    .filter(F.col("supplier_name").like("S%"))
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        "country_city",
        F.percent_rank().over(w).alias("rating_percent_rank"),
        F.round((F.col("rating") / F.sum("rating").over(w2)), 4).alias("rating_share"),
        F.sum("rating").over(w3).alias("cumulative_rating"),
    )
)

latest = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
    .select("id", "country", "rating_share")
)

result = (
    latest
    .withColumn("rating_share_tier", F.when(F.col("rating_share") >= 0.85, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["rating_share_tier", "country"], ["rating_share_tier"]],
        "rating_share_tier",
        "country",
    )
    .agg(
        F.countDistinct("country").alias("distinct_countries"),
        F.sum(F.when(F.col("country") == "JP", F.col("rating_share"))).alias("jp_total_rating_share"),
        F.percentile_approx("rating_share", [0.25, 0.5, 0.75]).alias("rating_share_quartiles"),
        F.count(F.when(F.col("country") == "US", 1)).alias("us_count"),
        F.percentile_approx("rating_share", 0.9).alias("rating_share_p90"),
        F.grouping("rating_share_tier").alias("gid"),
    )
    .filter(F.col("distinct_countries") < 1000)
    .orderBy(F.col("distinct_countries").asc_nulls_first())
)
