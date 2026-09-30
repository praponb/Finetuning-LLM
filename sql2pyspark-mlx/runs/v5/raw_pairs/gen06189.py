from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("country", "city")
w2 = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("city").orderBy(F.col("country_sum_rating").asc_nulls_first())
w_hist = Window.orderBy(F.col("first_known_rating").desc_nulls_last())
w6 = Window.partitionBy("product_category", "city").orderBy(F.col("country_sum_rating").asc_nulls_last())
w7 = Window.partitionBy("country", "product_category")
w8 = Window.partitionBy("country")

with_windows = (
    df_suppliers
    .withColumn("country_sum_rating", F.sum("rating").over(w_part))
    .withColumn("first_known_rating", F.first("rating", ignorenulls=True).over(w2))
)

with_last = (
    with_windows
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.launch_date").alias("product_launch_date"),
        F.col("c.category").alias("product_category"),
    )
)

matched_keys = (
    with_last
    .select("city")
    .intersectAll(
        df_suppliers
        .filter(F.col("rating").between(2.7, 3.7))
        .select("city")
    )
)

verified = (
    with_last
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

scored = (
    verified
    .select(
        "*",
        F.cume_dist().over(w4).alias("country_sum_rating_cume_dist"),
        F.rank().over(w_hist).alias("first_known_rating_rank"),
        F.percent_rank().over(w6).alias("country_sum_rating_percent_rank"),
    )
)

eligible = (
    scored
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.launch_date") <= F.col("s.product_launch_date")), "left_semi")
)

enriched = (
    eligible
    .withColumn("rating_share", F.round((F.col("rating") / F.sum("rating").over(w7)), 4))
    .withColumn("country_sum_first_known_rating_rank", F.sum("first_known_rating_rank").over(w8))
)

result = (
    enriched
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        "product_launch_date",
        "product_category",
        "country_sum_rating_cume_dist",
        "first_known_rating_rank",
        F.when(F.col("city").rlike("^(London|Chiang Mai)$"), "OTHER_ZONE").when(F.col("city").rlike("^(Bangkok|New York)$"), "CORE").otherwise("GROUP_A").alias("city_zone"),
    )
)
