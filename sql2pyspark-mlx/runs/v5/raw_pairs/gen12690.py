from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("monthly_fee", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("plan").orderBy("start_date_month_end", "id")
w3 = Window.partitionBy("customer_id")
w_hist = Window.partitionBy("city", "plan").orderBy(F.col("customer_id_stddev_samp_monthly_fee").desc(), "id")
w5 = Window.partitionBy("plan", "city").orderBy("monthly_fee_avg_6_prior", "id").rowsBetween(-3, Window.currentRow)
w_grp = Window.partitionBy("plan").orderBy("next_monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("customer_id", "city").orderBy("start_date_month_end", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.max("monthly_fee").alias("max_subscription_monthly_fee"))

valid = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        F.last_day("start_date").alias("start_date_month_end"),
    )
)

base = (
    valid
    .filter(F.col("customer_id").isNotNull() & (F.col("plan") == "premium"))
    .withColumn("monthly_fee_level", F.when(F.col("monthly_fee") >= 258, "High").when(F.col("monthly_fee") >= 90, "Medium").otherwise("Low"))
    .withColumn("monthly_fee_level_2", F.when(F.col("monthly_fee") >= 231, "High").when(F.col("monthly_fee") >= 85, "Medium").otherwise("Low"))
)

with_windows = (
    base
    .withColumn("monthly_fee_avg_6_prior", F.avg("monthly_fee").over(w))
    .withColumn("next_monthly_fee", F.lead("monthly_fee", 1).over(w2))
    .withColumn("customer_id_stddev_samp_monthly_fee", F.stddev_samp("monthly_fee").over(w3))
)

with_dims = (
    with_windows
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id").eqNullSafe(F.col("d.id")), "inner")
    .select("s.*", F.col("d.city"), F.col("d.name"))
)

with_windows_subscriptions = (
    with_dims
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        "monthly_fee_level",
        "monthly_fee_level_2",
        "customer_id_stddev_samp_monthly_fee",
        "city",
        "name",
        F.ntile(3).over(w_hist).alias("customer_id_stddev_samp_monthly_fee_bucket"),
        F.avg("customer_id_stddev_samp_monthly_fee").over(w5).alias("customer_id_stddev_samp_monthly_fee_avg_3"),
        F.sum("monthly_fee").over(w_grp).alias("cumulative_monthly_fee"),
        F.first("monthly_fee_avg_6_prior", ignorenulls=True).over(w7).alias("first_known_monthly_fee_avg_6_prior"),
    )
)

result = (
    with_windows_subscriptions
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "plan",
        "monthly_fee_level",
        "monthly_fee_level_2",
        "customer_id_stddev_samp_monthly_fee",
        "name",
        "customer_id_stddev_samp_monthly_fee_bucket",
        "customer_id_stddev_samp_monthly_fee_avg_3",
        F.when(F.col("name").rlike("^(Grace Kim|Henry Ford)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("name_zone"),
        F.col("max_subscription_monthly_fee"),
    )
    .orderBy(
        F.col("customer_id_stddev_samp_monthly_fee").desc_nulls_last(),
        F.col("plan").desc(),
    )
)
