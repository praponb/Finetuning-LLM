from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("country", "city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city").orderBy("rating", "id")
w_part = Window.orderBy("rating_plus_rating_bucket", "id")
w4 = Window.orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("prev_cumulative_rating").asc_nulls_first())
w6 = Window.orderBy("next_rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("seq_no", "id")
w8 = Window.orderBy("last_known_rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("prev_cumulative_rating_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_suppliers
    .filter(F.col("rating").between(2.6, 2.7))
    .select(
        "*",
        F.sum("rating").over(w_ordered).alias("cumulative_rating"),
        F.ntile(3).over(w2).alias("rating_bucket"),
    )
)

cleaned = (
    enriched
    .filter(F.col("city").isNotNull() & (F.lower(F.trim("supplier_name")) == "tokyo traders"))
    .select(
        "id",
        "rating",
        "cumulative_rating",
        (F.col("rating") + F.col("rating_bucket")).alias("rating_plus_rating_bucket"),
        F.regexp_replace("supplier_name", " ", "").alias("supplier_name_slug"),
    )
)

enriched_suppliers = (
    cleaned
    .withColumn("prev_cumulative_rating", F.lag("cumulative_rating").over(w_part))
    .withColumn("next_cumulative_rating", F.lead("cumulative_rating").over(w_part))
    .withColumn("next_rating", F.lead("rating").over(w_part))
    .withColumn("seq_no", F.row_number().over(w_part))
)

with_windows = (
    enriched_suppliers
    .withColumn("last_known_rating", F.last("rating", ignorenulls=True).over(w4))
    .withColumn("prev_cumulative_rating_cume_dist", F.cume_dist().over(w5))
    .withColumn("first_known_next_rating", F.first("next_rating", ignorenulls=True).over(w6))
    .withColumn("prev_next_rating", F.lag("next_rating", 1).over(w7))
)

metrics = (
    with_windows
    .select(
        "id",
        "rating",
        "rating_plus_rating_bucket",
        "supplier_name_slug",
        "prev_cumulative_rating",
        "next_rating",
        "seq_no",
        "last_known_rating",
        "prev_cumulative_rating_cume_dist",
        "first_known_next_rating",
        "prev_next_rating",
        F.sum("next_cumulative_rating").over(w8).alias("running_next_cumulative_rating"),
        F.last("cumulative_rating", ignorenulls=True).over(w9).alias("last_known_cumulative_rating"),
    )
)

result = (
    metrics
    .select(
        "id",
        "rating",
        "rating_plus_rating_bucket",
        "prev_cumulative_rating",
        "next_rating",
        "last_known_rating",
        "prev_cumulative_rating_cume_dist",
        "first_known_next_rating",
    )
    .orderBy("rating")
)
