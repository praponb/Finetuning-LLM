from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("segment", "city").orderBy("signup_date", "id")
w2 = Window.partitionBy("segment").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("segment").orderBy(F.col("prev_credit_limit").asc_nulls_first())
w4 = Window.partitionBy("segment")
w5 = Window.partitionBy("segment").orderBy(F.col("signup_date").desc(), "id")
w6 = Window.partitionBy("segment").orderBy(F.col("row_seq_2").desc_nulls_last())
w7 = Window.partitionBy("segment").orderBy("row_seq_2")
w8 = Window.partitionBy("segment").orderBy(F.col("prev_credit_limit_dense_rank").desc(), "id")
w9 = Window.partitionBy("segment").orderBy(F.col("row_seq_2").desc(), "id")
w10 = Window.partitionBy("segment").orderBy("prev_age", "id").rowsBetween(-6, -1)
w11 = Window.partitionBy("segment").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.partitionBy("segment").orderBy("rank_in_group", "id").rowsBetween(-2, -1)

scored = (
    df_customers
    .filter(F.lower(F.trim("name")) == "david lee")
    .select(
        "id",
        "name",
        "segment",
        "signup_date",
        "age",
        F.lag("credit_limit").over(w_hist).alias("prev_credit_limit"),
        F.lag("age").over(w_hist).alias("prev_age"),
        F.row_number().over(w_hist).alias("row_seq"),
    )
)

metrics = (
    scored
    .select(
        "*",
        F.last("segment", ignorenulls=True).over(w2).alias("last_known_segment"),
        F.dense_rank().over(w3).alias("prev_credit_limit_dense_rank"),
        F.round((F.col("age") / F.sum("age").over(w4)), 4).alias("age_share"),
        F.row_number().over(w5).alias("row_seq_2"),
    )
)

enriched = (
    metrics
    .select(
        "*",
        F.dense_rank().over(w6).alias("row_seq_2_dense_rank"),
        F.rank().over(w7).alias("row_seq_2_rank"),
    )
)

latest = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w8))
    .filter(F.col("rank_in_group") == 1)
)

metrics_customers = (
    latest
    .select(
        "id",
        "age",
        "prev_credit_limit",
        "prev_credit_limit_dense_rank",
        "age_share",
        "row_seq_2_rank",
        "rank_in_group",
        F.ntile(5).over(w9).alias("row_seq_2_bucket"),
        F.avg("row_seq").over(w10).alias("row_seq_avg_6_prior"),
        F.first("segment", ignorenulls=True).over(w11).alias("first_known_segment"),
        F.avg("row_seq_2").over(w12).alias("row_seq_2_avg_2_prior"),
    )
)

result = (
    metrics_customers
    .select(
        "id",
        "age",
        "prev_credit_limit",
        "prev_credit_limit_dense_rank",
        "age_share",
        "row_seq_2_rank",
        "row_seq_2_bucket",
        "row_seq_avg_6_prior",
        "first_known_segment",
    )
)
