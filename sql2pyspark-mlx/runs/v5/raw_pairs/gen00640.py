from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("rating", "id")
w_hist = Window.partitionBy("country").orderBy(F.col("rating").desc(), "id")
w_ordered = Window.partitionBy("country")
w4 = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.partitionBy("country", "city").orderBy(F.col("rating_bucket").desc())
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w7 = Window.partitionBy("country").orderBy("product_price", "id").rowsBetween(-2, Window.currentRow)
w8 = Window.partitionBy("country", "product_brand").orderBy(F.col("country_avg_rating").desc(), "id")
w_running = Window.partitionBy("country").orderBy("country_avg_rating_bucket", "id")
w10 = Window.orderBy("next_last3_sum_rating", "id")
w11 = Window.orderBy(F.col("seq_no").desc(), "id")
w_grp = Window.orderBy("next_last3_sum_rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w13 = Window.orderBy(F.col("last3_sum_rating").desc(), "id")
w14 = Window.orderBy("last3_max_rating_bucket", "id")

scored = (
    df_suppliers
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.lag("rating", 1).over(w).alias("prev_rating"),
        F.ntile(3).over(w_hist).alias("rating_bucket"),
        F.avg("rating").over(w_ordered).alias("country_avg_rating"),
    )
)

metrics = (
    scored
    .withColumn("last3_avg_rating", F.avg("rating").over(w4))
    .withColumn("last3_sum_rating", F.sum("rating").over(w4))
    .withColumn("last3_count_rating", F.count("rating").over(w4))
    .withColumn("rating_bucket_rank", F.rank().over(w5))
)

latest_match = (
    metrics
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.country",
        "s.rating",
        "s.rating_bucket",
        "s.country_avg_rating",
        "s.last3_sum_rating",
        "s.last3_count_rating",
        "s.rating_bucket_rank",
        F.col("c.brand").alias("product_brand"),
        F.col("c.price").alias("product_price"),
    )
)

scored_suppliers = (
    latest_match
    .select(
        "*",
        F.min("rating_bucket").over(w7).alias("last3_min_rating_bucket"),
        F.max("rating_bucket").over(w7).alias("last3_max_rating_bucket"),
        F.count("rating_bucket").over(w7).alias("last3_count_rating_bucket"),
        F.ntile(10).over(w8).alias("country_avg_rating_bucket"),
    )
)

with_windows = (
    scored_suppliers
    .select(
        "id",
        "last3_sum_rating",
        "rating_bucket_rank",
        "last3_min_rating_bucket",
        "last3_max_rating_bucket",
        F.lag("last3_sum_rating").over(w_running).alias("prev_last3_sum_rating"),
        F.lead("last3_sum_rating").over(w_running).alias("next_last3_sum_rating"),
        F.lag("country_avg_rating_bucket").over(w_running).alias("prev_country_avg_rating_bucket"),
        F.row_number().over(w_running).alias("seq_no"),
    )
)

metrics_suppliers = (
    with_windows
    .withColumn("seq_no_2", F.row_number().over(w10))
    .withColumn("seq_no_3", F.row_number().over(w11))
    .withColumn("last_known_last3_sum_rating", F.last("last3_sum_rating", ignorenulls=True).over(w_grp))
    .withColumn("last3_sum_rating_bucket", F.ntile(5).over(w13))
    .withColumn("next_rating_bucket_rank", F.lead("rating_bucket_rank", 1).over(w14))
)

result = (
    metrics_suppliers
    .select(
        "id",
        "last3_sum_rating",
        "last3_max_rating_bucket",
        "prev_last3_sum_rating",
        "next_last3_sum_rating",
        "prev_country_avg_rating_bucket",
        "seq_no_2",
        "seq_no_3",
        "last_known_last3_sum_rating",
    )
    .orderBy("next_last3_sum_rating", F.col("id").desc())
)
