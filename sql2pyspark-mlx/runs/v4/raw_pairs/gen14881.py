from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("age", "id")
w2 = Window.partitionBy("country").orderBy("credit_limit", "id")
w3 = Window.partitionBy("country")
w4 = Window.partitionBy("segment").orderBy("age", "id")
w5 = Window.partitionBy("country").orderBy(F.col("signup_date").desc(), "id")
w6 = Window.partitionBy("country").orderBy("age_bucket", "id").rowsBetween(-6, -1)
w7 = Window.partitionBy("segment")
w8 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.order_date").desc(), F.col("c.id").desc())
w9 = Window.partitionBy("order_channel").orderBy("corr_age_credit_limit", "id").rowsBetween(-2, Window.currentRow)
w10 = Window.orderBy(F.col("age_bucket").asc_nulls_first())
w11 = Window.partitionBy("city", "segment").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.partitionBy("segment").orderBy(F.col("credit_limit_share").desc_nulls_first())

with_windows = (
    df_customers
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.ntile(3).over(w2).alias("credit_limit_bucket"),
        F.corr("age", "credit_limit").over(w3).alias("corr_age_credit_limit"),
        F.ntile(5).over(w4).alias("age_bucket"),
        F.row_number().over(w5).alias("row_seq"),
    )
)

with_windows_customers = (
    with_windows
    .withColumn("credit_limit_bucket_avg_6_prior", F.avg("credit_limit_bucket").over(w6))
    .withColumn("credit_limit_share", F.round((F.col("credit_limit") / F.sum("credit_limit").over(w7)), 4))
)

latest_match = (
    with_windows_customers
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.channel").alias("order_channel"), F.col("c.discount"))
)

metrics = (
    latest_match
    .withColumn("seq_no_avg_2", F.avg("seq_no").over(w9))
    .withColumn("age_bucket_rank", F.rank().over(w10))
    .withColumn("first_known_order_channel", F.first("order_channel", ignorenulls=True).over(w11))
)

latest = (
    metrics
    .withColumn("rank_in_group", F.rank().over(w12))
    .filter(F.col("rank_in_group") == 1)
)

remaining_keys = (
    latest
    .select("country", "city")
    .intersectAll(
        df_customers
        .filter(F.col("email").like("%@gmail.com"))
        .select("country", "city")
    )
)

kept_rows = (
    latest
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "city",
        F.col("credit_limit_share").alias("CREDIT_LIMIT_SHARE_VAL"),
        F.col("corr_age_credit_limit").alias("CORR_AGE_CREDIT_LIMIT"),
        "discount",
    )
    .unpivot(
        ["city"],
        ["CREDIT_LIMIT_SHARE_VAL", "CORR_AGE_CREDIT_LIMIT", "discount"],
        "metric",
        "value",
    )
    .filter(F.col("value").isNotNull())
)
