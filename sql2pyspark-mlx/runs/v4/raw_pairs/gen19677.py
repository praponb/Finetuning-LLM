from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city", "country").orderBy("rating", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("country").orderBy(F.col("rating").desc(), "id")
w4 = Window.partitionBy("city", "country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("country", "city")
w6 = Window.partitionBy("country").orderBy(F.col("rating_bucket").desc(), "id")
w_recent = Window.partitionBy("country").orderBy("rating_bucket", "id")
w8 = Window.orderBy("cumulative_rating_2", "id")
w9 = Window.partitionBy("city").orderBy("next_country_min_rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("city", "country")
w11 = Window.partitionBy("city")

base = (
    df_suppliers
    .filter(~F.col("city").isin(["London", "Singapore", "Chiang Mai"]))
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.substring("city", 1, 2).alias("city_prefix"),
        F.initcap(F.trim("supplier_name")).alias("supplier_name_title"),
    )
)

scored = (
    base
    .select(
        "*",
        F.sum("rating").over(w).alias("cumulative_rating"),
        F.avg("rating").over(w2).alias("rating_avg_5_prior"),
        F.ntile(10).over(w3).alias("rating_bucket"),
        F.sum("rating").over(w4).alias("cumulative_rating_2"),
        F.min("rating").over(w5).alias("country_min_rating"),
    )
)

enriched = (
    scored
    .withColumn("rating_bucket_bucket", F.ntile(3).over(w6))
    .withColumn("row_seq", F.row_number().over(w_recent))
    .withColumn("next_country_min_rating", F.lead("country_min_rating", 2).over(w8))
)

scored_suppliers = (
    enriched
    .select(
        "*",
        F.last("next_country_min_rating", ignorenulls=True).over(w9).alias("last_known_next_country_min_rating"),
        F.round((F.col("country_min_rating") / F.sum("country_min_rating").over(w10)), 4).alias("country_min_rating_share"),
        F.stddev_samp("rating").over(w11).alias("city_stddev_samp_rating"),
    )
)

result = (
    scored_suppliers
    .select(
        "id",
        "supplier_name",
        "country",
        "rating",
        "city_prefix",
        "supplier_name_title",
        "cumulative_rating",
        "rating_bucket",
        "rating_bucket_bucket",
    )
)
