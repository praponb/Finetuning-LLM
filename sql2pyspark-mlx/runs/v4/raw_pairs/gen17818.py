from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("status", "customer_id")
w3 = Window.partitionBy("customer_id", "segment").orderBy("age")
w4 = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").desc(), "id")
w5 = Window.partitionBy("segment", "city").orderBy(F.col("credit_limit").desc(), "id")
w6 = Window.partitionBy("status", "plan").orderBy("start_date", "id").rowsBetween(-3, Window.currentRow)
w7 = Window.partitionBy("customer_id").orderBy("monthly_fee", "id")
w8 = Window.partitionBy("segment").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .withColumn("plan_group", F.when(F.col("plan").isin(["enterprise", "basic"]), "Group A").otherwise("Other"))
)

enriched = (
    valid
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.credit_limit"), F.col("p.segment"), F.col("p.city"))
)

joined = (
    enriched
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "left")
    .select("e.*", F.col("c.age"))
)

metrics = (
    joined
    .select(
        "*",
        F.max("monthly_fee").over(w).alias("city_max_monthly_fee"),
        F.round((F.col("age") / F.sum("age").over(w2)), 4).alias("age_share"),
        F.dense_rank().over(w3).alias("age_dense_rank"),
        F.ntile(3).over(w4).alias("monthly_fee_bucket"),
        F.ntile(3).over(w5).alias("credit_limit_bucket"),
    )
)

with_windows = (
    metrics
    .withColumn("credit_limit_bucket_avg_3", F.avg("credit_limit_bucket").over(w6))
    .withColumn("prev_credit_limit", F.lag("credit_limit", 1).over(w7))
    .withColumn("cumulative_city_max_monthly_fee", F.sum("city_max_monthly_fee").over(w8))
)

result = (
    with_windows
    .groupBy("plan")
    .pivot("segment", ["Consumer", "SMB"])
    .agg(F.max("age_share").alias("total"), F.count(F.lit(1)).alias("item_count"))
)
