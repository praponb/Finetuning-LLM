from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id")
w2 = Window.partitionBy("city", "country").orderBy("next_rating", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("country", "city").orderBy("prev_rating", "id")
w4 = Window.partitionBy("country").orderBy(F.col("prev_rating").desc(), "id")
w5 = Window.partitionBy("city").orderBy("prev_rating", "id")
w_grp = Window.partitionBy("city").orderBy("prev_rating_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.when(F.col("country") == "UK", 1).otherwise(0).alias("is_uk"),
    )
)

metrics = (
    cleaned
    .withColumn("prev_rating", F.lag("rating").over(w))
    .withColumn("next_rating", F.lead("rating").over(w))
    .withColumn("next_is_uk", F.lead("is_uk").over(w))
    .withColumn("row_seq", F.row_number().over(w))
)

enriched = (
    metrics
    .filter(F.lower(F.trim("supplier_name")) == "lion city imports")
    .select(
        "id",
        "city",
        "rating",
        "is_uk",
        "prev_rating",
        "next_rating",
        F.avg("row_seq").over(w2).alias("row_seq_avg_5_prior"),
        F.lag("rating", 2, 0).over(w3).alias("prev_rating_2"),
        F.ntile(10).over(w4).alias("prev_rating_bucket"),
    )
)

scored = (
    enriched
    .select(
        "*",
        F.lag("prev_rating").over(w5).alias("prev_prev_rating"),
        F.lead("prev_rating").over(w5).alias("next_prev_rating"),
        F.lead("prev_rating_2").over(w5).alias("next_prev_rating_2"),
        F.row_number().over(w5).alias("row_seq"),
    )
)

eligible = (
    scored
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.supplier_id").isNull()), "left_anti")
)

result = (
    eligible
    .select(
        "id",
        "city",
        "rating",
        "is_uk",
        "next_rating",
        "row_seq_avg_5_prior",
        "prev_rating_2",
        F.sum("next_prev_rating").over(w_grp).alias("running_sum_next_prev_rating"),
        F.avg("next_prev_rating").over(w_grp).alias("running_avg_next_prev_rating"),
        F.max("next_prev_rating").over(w_grp).alias("running_max_next_prev_rating"),
        F.min("next_prev_rating").over(w_grp).alias("running_min_next_prev_rating"),
    )
)
