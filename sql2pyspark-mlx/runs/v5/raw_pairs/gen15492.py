from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc())
w2 = Window.partitionBy("city", "country").orderBy(F.col("rating").desc(), "id")
w_part = Window.partitionBy("city").orderBy("is_singapore", "id")
w_ordered = Window.partitionBy("city").orderBy("rating_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "*",
        F.when(F.col("city") == "Singapore", 1).otherwise(0).alias("is_singapore"),
        F.trim("supplier_name").alias("clean_supplier_name"),
    )
)

metrics = (
    filtered
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        "is_singapore",
        F.rank().over(w).alias("rating_rank"),
        F.ntile(10).over(w2).alias("rating_bucket"),
    )
)

metrics_suppliers = (
    metrics
    .select(
        "*",
        F.lead("rating").over(w_part).alias("next_rating"),
        F.lag("rating").over(w_part).alias("prev_rating"),
        F.lead("rating_bucket").over(w_part).alias("next_rating_bucket"),
        F.row_number().over(w_part).alias("seq_no"),
        F.first("rating_rank", ignorenulls=True).over(w_ordered).alias("first_known_rating_rank"),
    )
)

filtered_suppliers = (
    metrics_suppliers
    .filter(F.col("next_rating_bucket").between(5, 8))
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        "is_singapore",
        "rating_rank",
        "rating_bucket",
        "prev_rating",
        "next_rating_bucket",
        "seq_no",
        "first_known_rating_rank",
        (F.col("rating") - F.col("first_known_rating_rank")).alias("rating_minus_first_known_rating_rank"),
        F.coalesce("city", F.lit("OTHER")).alias("city_filled"),
    )
)

result = (
    filtered_suppliers
    .groupBy(
        F.when(F.col("first_known_rating_rank") >= 9, "LARGE").otherwise("SMALL").alias("first_known_rating_rank_tier"),
        F.when(F.col("rating_rank") >= 8, "PREMIUM").otherwise("STANDARD").alias("rating_rank_tier"),
        "seq_no",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("prev_rating", 0.95).alias("prev_rating_p95"),
        F.count_if(F.col("city") == "Chiang Mai").alias("chiang_mai_events"),
        F.count("*").alias("num_rows"),
    )
    .filter(F.col("prev_rating_p95") >= 0)
)
