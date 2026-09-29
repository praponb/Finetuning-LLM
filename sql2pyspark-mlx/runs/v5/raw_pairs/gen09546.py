from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy("rating")
w_running = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(-3, -1)
w4 = Window.orderBy(F.col("running_count_rating").desc())
w5 = Window.orderBy("running_min_rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("running_avg_rating", "id")
w7 = Window.orderBy(F.col("running_avg_rating").desc_nulls_first())
w8 = Window.orderBy("running_count_rating_percent_rank", "id").rowsBetween(-6, Window.currentRow)

base = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.trim("country").alias("clean_country"),
    )
)

deduped = (
    base
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "supplier_name", "city", "rating", "clean_country")
)

scored = (
    deduped
    .filter(F.col("city").isin(["Berlin", "Bangkok"]))
    .select(
        "id",
        "supplier_name",
        F.count("rating").over(w_running).alias("running_count_rating"),
        F.min("rating").over(w_running).alias("running_min_rating"),
        F.avg("rating").over(w_running).alias("running_avg_rating"),
        F.avg("rating").over(w3).alias("rating_avg_3_prior"),
    )
)

with_windows = (
    scored
    .select(
        "id",
        "running_count_rating",
        "running_avg_rating",
        F.percent_rank().over(w4).alias("running_count_rating_percent_rank"),
        F.last("rating_avg_3_prior", ignorenulls=True).over(w5).alias("last_known_rating_avg_3_prior"),
        F.lag("running_count_rating", 1).over(w6).alias("prev_running_count_rating"),
        F.dense_rank().over(w7).alias("running_avg_rating_dense_rank"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "running_count_rating",
        "running_avg_rating",
        "last_known_rating_avg_3_prior",
        "prev_running_count_rating",
        F.max("running_avg_rating").over(w8).alias("last7_max_running_avg_rating"),
        F.min("running_avg_rating").over(w8).alias("last7_min_running_avg_rating"),
        F.count("running_avg_rating").over(w8).alias("last7_count_running_avg_rating"),
    )
)

result = (
    metrics
    .select(
        "id",
        "running_count_rating",
        "running_avg_rating",
        "last7_min_running_avg_rating",
        "last7_count_running_avg_rating",
    )
    .orderBy(F.col("running_avg_rating").desc_nulls_last())
)
