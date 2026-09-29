from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "plan").orderBy("start_date", "id")
w_part = Window.partitionBy("customer_id", "plan").orderBy("end_date", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee_int_avg_6").asc_nulls_first(), "id")
w4 = Window.partitionBy("plan").orderBy("start_date", "id")

valid = (
    df_subscriptions
    .filter(F.col("end_date") >= F.to_date(F.lit("2024-02-06")))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        F.col("monthly_fee").cast("int").alias("monthly_fee_int"),
        F.add_months("end_date", 6).alias("end_date_plus_months"),
    )
)

scored = (
    valid
    .withColumn("next_monthly_fee", F.lead("monthly_fee", 1).over(w))
    .withColumn("monthly_fee_int_avg_6", F.avg("monthly_fee_int").over(w_part))
)

base = (
    scored
    .filter((F.col("start_date") > F.to_date(F.lit("2023-10-07"))) & F.col("customer_id").isNotNull())
    .select(
        "*",
        F.when(F.col("monthly_fee_int_avg_6") >= 81, "High").when(F.col("monthly_fee_int_avg_6") >= 14, "Medium").otherwise("Low").alias("monthly_fee_int_avg_6_tier"),
        F.date_sub("start_date", 8).alias("start_date_minus_8d"),
    )
)

deduped = (
    base
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

deduped_subscriptions = (
    deduped
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

filtered = (
    deduped_subscriptions
    .filter((F.col("end_date") >= F.lit("2024-02-17").cast("date")) & F.col("customer_id").isNotNull() & (F.col("monthly_fee_int_avg_6") < 61))
    .select(
        "id",
        "plan",
        "end_date",
        "monthly_fee",
        "monthly_fee_int",
        "end_date_plus_months",
        "next_monthly_fee",
        "monthly_fee_int_avg_6",
        "monthly_fee_int_avg_6_tier",
        "start_date_minus_8d",
        "rank_in_group",
        (F.col("monthly_fee_int_avg_6") + F.col("next_monthly_fee")).alias("monthly_fee_int_avg_6_plus_next_monthly_fee"),
        (F.col("monthly_fee_int") * F.col("monthly_fee")).alias("monthly_fee_int_times_monthly_fee"),
    )
)

result = (
    filtered
    .select(
        "id",
        "end_date",
        "monthly_fee",
        "end_date_plus_months",
        "next_monthly_fee",
        "monthly_fee_int_avg_6",
        "monthly_fee_int_avg_6_tier",
        "monthly_fee_int_avg_6_plus_next_monthly_fee",
        "monthly_fee_int_times_monthly_fee",
    )
    .orderBy(F.col("id").desc_nulls_last())
)
