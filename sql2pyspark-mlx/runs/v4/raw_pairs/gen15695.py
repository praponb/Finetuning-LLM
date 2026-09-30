from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("credit_limit_int").desc(), "id")
w2 = Window.partitionBy("segment", "country").orderBy("credit_limit_int", "id")
w3 = Window.partitionBy("city")
w4 = Window.partitionBy("segment").orderBy("credit_limit_int", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("city", "segment").orderBy(F.col("credit_limit_int_bucket").asc_nulls_last())
w6 = Window.partitionBy("city").orderBy(F.col("corr_age_credit_limit").asc_nulls_last())
w7 = Window.partitionBy("city").orderBy("credit_limit_int_bucket", "id")
w8 = Window.partitionBy("segment", "city").orderBy("credit_limit", "id")
w9 = Window.partitionBy("country").orderBy("latest_rank", "id")
w10 = Window.partitionBy("city").orderBy("corr_age_credit_limit_dense_rank", "id").rowsBetween(-3, -1)
w11 = Window.partitionBy("city").orderBy(F.col("credit_limit_int_bucket").desc())

valid = (
    df_customers
    .filter(F.col("segment").isNotNull() & F.col("credit_limit").between(10800, 12200))
    .select(
        "id",
        "name",
        "country",
        "city",
        "segment",
        "is_active",
        "credit_limit",
        "age",
        F.col("credit_limit").cast("int").alias("credit_limit_int"),
    )
)

metrics = (
    valid
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.ntile(4).over(w2).alias("credit_limit_int_bucket"),
        F.sum("age").over(w3).alias("city_sum_age"),
        F.corr("age", "credit_limit").over(w3).alias("corr_age_credit_limit"),
        F.sum("age").over(w4).alias("running_age"),
    )
)

latest = (
    metrics
    .withColumn("latest_rank", F.rank().over(w5))
    .filter(F.col("latest_rank") == 1)
)

with_windows = (
    latest
    .withColumn("corr_age_credit_limit_dense_rank", F.dense_rank().over(w6))
    .withColumn("next_credit_limit_int_bucket", F.lead("credit_limit_int_bucket", 1).over(w7))
    .withColumn("next_credit_limit_int", F.lead("credit_limit_int", 2).over(w8))
    .withColumn("next_row_seq", F.lead("row_seq", 1).over(w9))
)

not_flagged = (
    with_windows
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.order_date") >= "2023-03-01"), "left_anti")
)

filtered = (
    not_flagged
    .filter(F.col("segment").isNotNull() & (F.col("city_sum_age") >= 254))
    .select(
        "id",
        "city",
        "segment",
        "is_active",
        "credit_limit",
        "age",
        "credit_limit_int",
        "credit_limit_int_bucket",
        "city_sum_age",
        "corr_age_credit_limit",
        "running_age",
        "corr_age_credit_limit_dense_rank",
        "next_credit_limit_int_bucket",
        "next_row_seq",
        (F.col("credit_limit_int") * F.col("next_credit_limit_int_bucket")).alias("credit_limit_int_times_next_credit_limit_int_bucket"),
    )
)

with_windows_customers = (
    filtered
    .withColumn("credit_limit_int_times_next_credit_limit_int_bucket_avg_3_prior", F.avg("credit_limit_int_times_next_credit_limit_int_bucket").over(w10))
    .withColumn("credit_limit_int_bucket_cume_dist", F.cume_dist().over(w11))
)

result = (
    with_windows_customers
    .select(
        "id",
        "city",
        "segment",
        "is_active",
        "age",
        "city_sum_age",
        "corr_age_credit_limit",
        "corr_age_credit_limit_dense_rank",
        "next_credit_limit_int_bucket",
        F.when(F.col("city").rlike("^(Singapore|Chiang Mai)$"), "CORE").otherwise("OTHER_ZONE").alias("city_zone"),
    )
    .orderBy(F.col("corr_age_credit_limit_dense_rank").desc_nulls_first())
)
