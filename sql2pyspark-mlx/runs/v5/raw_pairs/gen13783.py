from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy(F.col("rating").desc_nulls_first())
w_recent = Window.orderBy(F.col("rating_filled").desc(), "id")
w3 = Window.orderBy("rating", "id").rowsBetween(-6, -1)
w4 = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("row_seq_avg_6_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("row_seq_avg_6_prior")
w7 = Window.orderBy(F.col("row_seq_avg_6_prior").desc_nulls_last())
w8 = Window.orderBy("first_known_cumulative_rating_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("row_seq_avg_6_prior_dense_rank_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy(F.col("rating_rank").desc_nulls_first())
w11 = Window.orderBy("first_known_cumulative_rating_filled", "id")
w12 = Window.orderBy("row_seq_avg_6_prior_dense_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_suppliers
    .filter((~F.col("city").eqNullSafe(F.lit("London"))) & F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.when(F.col("rating") >= 3.6, "High").when(F.col("rating") >= 2.1, "Medium").otherwise("Low").alias("rating_band"),
    )
)

cleaned = (
    filtered
    .filter(F.col("supplier_name").isNotNull())
    .select(
        "id",
        "supplier_name",
        "rating",
        "rating_band",
        F.coalesce("rating", F.lit(0)).alias("rating_filled"),
    )
)

scored = (
    cleaned
    .withColumn("rating_rank", F.rank().over(w_grp))
    .withColumn("row_seq", F.row_number().over(w_recent))
)

enriched = (
    scored
    .select(
        "id",
        "rating",
        "rating_band",
        "rating_rank",
        F.avg("row_seq").over(w3).alias("row_seq_avg_6_prior"),
        F.sum("rating_filled").over(w4).alias("cumulative_rating_filled"),
    )
)

scored_suppliers = (
    enriched
    .filter(F.col("cumulative_rating_filled").between(113, 197))
    .select(
        "id",
        "rating_rank",
        F.first("cumulative_rating_filled", ignorenulls=True).over(w5).alias("first_known_cumulative_rating_filled"),
        F.dense_rank().over(w6).alias("row_seq_avg_6_prior_dense_rank"),
        F.dense_rank().over(w7).alias("row_seq_avg_6_prior_dense_rank_2"),
    )
)

with_windows = (
    scored_suppliers
    .select(
        "id",
        "first_known_cumulative_rating_filled",
        F.last("rating_rank", ignorenulls=True).over(w8).alias("last_known_rating_rank"),
        F.last("first_known_cumulative_rating_filled", ignorenulls=True).over(w9).alias("last_known_first_known_cumulative_rating_filled"),
        F.rank().over(w10).alias("rating_rank_rank"),
        F.lead("row_seq_avg_6_prior_dense_rank_2", 1).over(w11).alias("next_row_seq_avg_6_prior_dense_rank_2"),
        F.last("rating_rank", ignorenulls=True).over(w12).alias("last_known_rating_rank_2"),
    )
)

eligible = (
    with_windows
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (~F.col("c.product_name").like("O%")), "left_anti")
)

result = (
    eligible
    .groupBy(
        F.when(F.col("last_known_rating_rank_2") >= 8, "PREMIUM").otherwise("STANDARD").alias("last_known_rating_rank_2_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.percentile_approx("first_known_cumulative_rating_filled", 0.9).alias("first_known_cumulative_rating_filled_p90"),
        F.round(F.avg("last_known_rating_rank_2"), 2).alias("avg_last_known_rating_rank_2"),
        F.round(F.avg("next_row_seq_avg_6_prior_dense_rank_2"), 2).alias("avg_next_row_seq_avg_6_prior_dense_rank_2"),
    )
    .orderBy("num_rows")
)
