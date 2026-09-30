from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())
w2 = Window.orderBy("rating_whole", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.orderBy(F.col("rating_whole").desc_nulls_last())
w4 = Window.orderBy("rating_whole", "id").rowsBetween(-2, -1)
w5 = Window.orderBy(F.col("rating_whole_percent_rank").asc_nulls_first())
w6 = Window.orderBy("product_stock_qty_avg_2", "id")
w7 = Window.orderBy("rating_whole_percent_rank_cume_dist", "id")
w8 = Window.orderBy(F.col("last3_min_rating_whole").desc_nulls_last())
w9 = Window.orderBy("last3_min_rating_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .withColumn("rating_whole", F.ceil("rating"))
    .withColumn("country_prefix", F.substring("country", 1, 4))
)

with_last = (
    base
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.supplier_name",
        "s.rating_whole",
        "s.country_prefix",
        F.col("c.stock_qty").alias("product_stock_qty"),
    )
)

with_windows = (
    with_last
    .select(
        "id",
        "rating_whole",
        F.avg("product_stock_qty").over(w2).alias("product_stock_qty_avg_2"),
        F.percent_rank().over(w3).alias("rating_whole_percent_rank"),
        F.avg("product_stock_qty").over(w4).alias("product_stock_qty_avg_2_prior"),
    )
)

scored = (
    with_windows
    .select(
        "id",
        "rating_whole",
        "product_stock_qty_avg_2",
        "rating_whole_percent_rank",
        "product_stock_qty_avg_2_prior",
        F.min("rating_whole").over(w2).alias("last3_min_rating_whole"),
        F.sum("rating_whole").over(w2).alias("last3_sum_rating_whole"),
        F.count("rating_whole").over(w2).alias("last3_count_rating_whole"),
        F.cume_dist().over(w5).alias("rating_whole_percent_rank_cume_dist"),
    )
)

valid = (
    scored
    .filter(F.col("last3_sum_rating_whole") < 160)
    .select(
        "*",
        F.when(F.col("rating_whole") >= 73, "High").when(F.col("rating_whole") >= 25, "Medium").otherwise("Low").alias("rating_whole_band"),
        F.when(F.col("last3_count_rating_whole") >= 18, "High").when(F.col("last3_count_rating_whole") >= 8, "Medium").otherwise("Low").alias("last3_count_rating_whole_tier"),
    )
)

result = (
    valid
    .select(
        "id",
        "product_stock_qty_avg_2",
        "rating_whole_percent_rank",
        "last3_sum_rating_whole",
        "last3_count_rating_whole",
        "last3_count_rating_whole_tier",
        F.row_number().over(w6).alias("event_seq"),
        F.lag("rating_whole_percent_rank_cume_dist", 1, 0).over(w7).alias("prev_rating_whole_percent_rank_cume_dist"),
        F.rank().over(w8).alias("last3_min_rating_whole_rank"),
        F.first("rating_whole_percent_rank", ignorenulls=True).over(w9).alias("first_known_rating_whole_percent_rank"),
    )
)
