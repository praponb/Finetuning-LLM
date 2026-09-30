from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("credit_limit", "id")
w_running = Window.partitionBy("country").orderBy("credit_limit", "id")
w3 = Window.partitionBy("city", "country").orderBy(F.col("age").desc(), "id")
w4 = Window.partitionBy("segment").orderBy(F.col("next_credit_limit").desc_nulls_first(), "id")
w5 = Window.partitionBy("segment").orderBy("credit_limit_bucket", "id")
w6 = Window.partitionBy("segment").orderBy(F.col("credit_limit_bucket").desc_nulls_first())
w7 = Window.partitionBy("segment")
w8 = Window.partitionBy("segment").orderBy("next_age_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("credit_limit_bucket_bucket", "id").rowsBetween(-5, -1)
w10 = Window.orderBy("segment_min_credit_limit_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_customers
    .select(
        "*",
        F.lead("credit_limit", 1).over(w).alias("next_credit_limit"),
        F.ntile(4).over(w_running).alias("credit_limit_bucket"),
        F.ntile(3).over(w3).alias("age_bucket"),
    )
)

ranked = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "name",
        "email",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit_bucket",
        "age_bucket",
    )
)

enriched = (
    ranked
    .select(
        "id",
        "name",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit_bucket",
        F.ntile(10).over(w5).alias("credit_limit_bucket_bucket"),
        F.lead("age_bucket", 1).over(w5).alias("next_age_bucket"),
        F.dense_rank().over(w6).alias("credit_limit_bucket_dense_rank"),
        F.min("credit_limit_bucket").over(w7).alias("segment_min_credit_limit_bucket"),
    )
)

not_flagged = (
    enriched
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.shipping_fee") >= 34), "left_anti")
)

scored = (
    not_flagged
    .select(
        "id",
        "signup_date",
        "is_active",
        "credit_limit_bucket_bucket",
        "next_age_bucket",
        "credit_limit_bucket_dense_rank",
        F.sum("credit_limit_bucket").over(w8).alias("running_credit_limit_bucket"),
        F.avg("segment_min_credit_limit_bucket").over(w9).alias("segment_min_credit_limit_bucket_avg_5_prior"),
        F.first("credit_limit_bucket", ignorenulls=True).over(w10).alias("first_known_credit_limit_bucket"),
    )
)

kept_keys = (
    scored
    .select("id")
    .exceptAll(
        df_customers
        .filter(F.col("age") > 59)
        .select("id")
    )
)

kept_rows = (
    scored
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .withColumn("running_credit_limit_bucket_tier", F.when(F.col("running_credit_limit_bucket") >= 33, "PREMIUM").otherwise("STANDARD"))
    .rollup("running_credit_limit_bucket_tier")
    .agg(
        F.count("*").alias("cnt"),
        F.count("*").alias("cnt_2"),
        F.countDistinct("id").alias("distinct_ids"),
        F.grouping("running_credit_limit_bucket_tier").alias("gid"),
    )
)
