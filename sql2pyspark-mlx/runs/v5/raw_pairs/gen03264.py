from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy("rating", "id")
w_running_2 = Window.partitionBy("city").orderBy("rating", "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())
w_part = Window.partitionBy("product_category").orderBy("rating_bucket", "id")
w5 = Window.partitionBy("product_category", "country")
w6 = Window.partitionBy("country").orderBy(F.col("prev_rating_bucket_p50").desc())

scored = (
    df_suppliers
    .filter(F.col("country") == "JP")
    .select(
        "id",
        "country",
        "city",
        F.ntile(3).over(w_running).alias("rating_bucket"),
        F.lag("rating", 1).over(w_running_2).alias("prev_rating"),
    )
)

with_last = (
    scored
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.country",
        "s.rating_bucket",
        "s.prev_rating",
        F.col("c.category").alias("product_category"),
    )
)

kept = (
    with_last
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.cost") >= 660), "left_semi")
)

scored_suppliers = (
    kept
    .select(
        "*",
        F.lead("prev_rating").over(w_part).alias("next_prev_rating"),
        F.lag("rating_bucket").over(w_part).alias("prev_rating_bucket"),
        F.lead("rating_bucket").over(w_part).alias("next_rating_bucket"),
        F.row_number().over(w_part).alias("seq_no"),
        F.sum("prev_rating").over(w5).alias("product_category_sum_prev_rating"),
    )
)

kept_suppliers = (
    scored_suppliers
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.price") > 1050), "left_anti")
)

result = (
    kept_suppliers
    .groupBy(
        "country",
        F.when(F.col("next_rating_bucket") >= 9, "PREMIUM").otherwise("STANDARD").alias("next_rating_bucket_tier"),
    )
    .agg(
        F.percentile_approx("prev_rating_bucket", 0.5).alias("prev_rating_bucket_p50"),
        F.avg("rating_bucket").alias("avg_rating_bucket"),
        F.count_if(F.col("product_category") == "Fashion").alias("fashion_events"),
    )
    .filter(F.col("fashion_events") < 1000)
    .withColumn("prev_rating_bucket_p50_dense_rank", F.dense_rank().over(w6))
    .orderBy(F.col("avg_rating_bucket").desc())
)
