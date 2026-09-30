from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w_part = Window.partitionBy("city", "country").orderBy(F.col("rating_share").desc(), "id")
w3 = Window.partitionBy("country", "city").orderBy(F.col("rating").desc_nulls_first())
w4 = Window.orderBy(F.col("city_sum_rating").desc(), "id")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())
w6 = Window.partitionBy("product_category").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("product_category")
w_grp = Window.orderBy("running_avg_rating_percent_rank", "id").rowsBetween(-2, Window.currentRow)

scored = (
    df_suppliers
    .select(
        "id",
        "country",
        "city",
        "rating",
        F.round((F.col("rating") / F.sum("rating").over(w)), 4).alias("rating_share"),
        F.sum("rating").over(w).alias("city_sum_rating"),
    )
)

enriched = (
    scored
    .select(
        "id",
        "country",
        F.row_number().over(w_part).alias("event_seq"),
        F.percent_rank().over(w3).alias("rating_percent_rank"),
        F.row_number().over(w4).alias("row_seq"),
    )
)

kept = (
    enriched
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.launch_date").between("2024-06-01", "2025-03-25")), "left_anti")
)

latest_match = (
    kept
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.country",
        "s.event_seq",
        "s.rating_percent_rank",
        F.col("c.category").alias("product_category"),
        F.col("c.price"),
    )
)

scored_suppliers = (
    latest_match
    .select(
        "id",
        "price",
        F.sum("rating_percent_rank").over(w6).alias("running_sum_rating_percent_rank"),
        F.count("rating_percent_rank").over(w6).alias("running_count_rating_percent_rank"),
        F.min("rating_percent_rank").over(w6).alias("running_min_rating_percent_rank"),
        F.avg("rating_percent_rank").over(w6).alias("running_avg_rating_percent_rank"),
        F.stddev_samp("event_seq").over(w7).alias("product_category_stddev_samp_event_seq"),
    )
)

scored_suppliers_2 = (
    scored_suppliers
    .withColumn("last3_sum_running_sum_rating_percent_rank", F.sum("running_sum_rating_percent_rank").over(w_grp))
    .withColumn("last3_min_running_sum_rating_percent_rank", F.min("running_sum_rating_percent_rank").over(w_grp))
    .withColumn("last3_max_running_sum_rating_percent_rank", F.max("running_sum_rating_percent_rank").over(w_grp))
)

result = (
    scored_suppliers_2
    .select(
        "id",
        "price",
        "running_sum_rating_percent_rank",
        "running_count_rating_percent_rank",
        "running_min_rating_percent_rank",
        "last3_sum_running_sum_rating_percent_rank",
        "last3_min_running_sum_rating_percent_rank",
        "last3_max_running_sum_rating_percent_rank",
    )
    .orderBy(F.col("id").desc())
)
