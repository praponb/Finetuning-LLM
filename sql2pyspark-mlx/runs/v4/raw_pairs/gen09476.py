from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipping_fee").desc(), F.col("c.id").desc())
w2 = Window.orderBy("signup_date", "id").rowsBetween(-2, -1)
w3 = Window.orderBy(F.col("credit_limit").asc_nulls_first())

valid = (
    df_customers
    .filter(F.col("city").isNotNull())
    .select("*", F.round((F.col("credit_limit") * 0.9), 2).alias("credit_limit_scaled"))
)

latest_match = (
    valid
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.discount").alias("order_discount"),
        F.col("c.status").alias("order_status"),
    )
)

scored = (
    latest_match
    .select(
        "*",
        F.avg("order_discount").over(w2).alias("order_discount_avg_2_prior"),
        F.dense_rank().over(w3).alias("credit_limit_dense_rank"),
    )
)

result = (
    scored
    .groupBy("segment")
    .agg(
        F.count("*").alias("num_rows"),
        F.avg(F.when(F.col("segment") == "Corporate", F.col("order_discount_avg_2_prior"))).alias("corporate_avg_order_discount_avg_2_prior"),
        F.count(F.when(F.col("segment") == "SMB", 1)).alias("smb_rows"),
        F.round(F.sum("order_discount") / F.nullif(F.count(F.when(F.col("country") == "UK", 1)), F.lit(0)), 2).alias("order_discount_per_uk"),
        F.percentile_approx("order_discount_avg_2_prior", 0.9).alias("order_discount_avg_2_prior_p90"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
    .orderBy(F.col("order_discount_per_uk").desc_nulls_first())
)
