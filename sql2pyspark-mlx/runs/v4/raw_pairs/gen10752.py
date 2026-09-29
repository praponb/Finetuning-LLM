from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("credit_limit", "id")
w2 = Window.partitionBy("segment").orderBy("credit_limit", "id")
w3 = Window.partitionBy("country").orderBy("signup_date", "id")
w4 = Window.partitionBy("country").orderBy("row_num", "id").rowsBetween(-3, -1)
w5 = Window.partitionBy("country").orderBy(F.col("next_credit_limit").desc(), "id")
w_hist = Window.partitionBy("city")
w7 = Window.partitionBy("city").orderBy(F.col("signup_date").desc(), "id")
w8 = Window.partitionBy("city", "segment").orderBy(F.col("credit_limit_bucket_avg_3_prior").desc(), "id")
w9 = Window.partitionBy("city", "segment").orderBy("signup_date", "id")
w10 = Window.partitionBy("segment").orderBy(F.col("next_credit_limit").desc())
w11 = Window.partitionBy("segment", "city").orderBy("credit_limit_bucket_avg_3_prior_bucket", "id").rowsBetween(-5, Window.currentRow)
w12 = Window.partitionBy("city", "country").orderBy("event_seq", "id")
w13 = Window.partitionBy("country").orderBy("credit_limit_bucket_avg_3_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w14 = Window.partitionBy("segment").orderBy("event_seq", "id").rowsBetween(-5, Window.currentRow)
w15 = Window.orderBy("row_num", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_customers
    .withColumn("next_credit_limit", F.lead("credit_limit", 2).over(w))
    .withColumn("credit_limit_bucket", F.ntile(3).over(w2))
)

deduped = (
    with_windows
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
)

with_windows_customers = (
    deduped
    .select(
        "*",
        F.avg("credit_limit_bucket").over(w4).alias("credit_limit_bucket_avg_3_prior"),
        F.ntile(4).over(w5).alias("next_credit_limit_bucket"),
        F.corr("credit_limit", "age").over(w_hist).alias("corr_credit_limit_age"),
    )
)

with_windows_customers_2 = (
    with_windows_customers
    .withColumn("event_seq", F.row_number().over(w7))
    .withColumn("credit_limit_bucket_avg_3_prior_bucket", F.ntile(10).over(w8))
)

enriched = (
    with_windows_customers_2
    .withColumn("prev_next_credit_limit", F.lag("next_credit_limit", 1, 0).over(w9))
    .withColumn("next_credit_limit_rank", F.rank().over(w10))
    .withColumn("event_seq_avg_5", F.avg("event_seq").over(w11))
)

enriched_customers = (
    enriched
    .select(
        "*",
        F.row_number().over(w12).alias("seq_no"),
        F.first("credit_limit_bucket", ignorenulls=True).over(w13).alias("first_known_credit_limit_bucket"),
        F.avg("credit_limit_bucket").over(w14).alias("credit_limit_bucket_avg_5"),
        F.first("credit_limit_bucket", ignorenulls=True).over(w15).alias("first_known_credit_limit_bucket_2"),
    )
)

result = (
    enriched_customers
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
    )
    .orderBy(F.col("segment").asc_nulls_first())
)
