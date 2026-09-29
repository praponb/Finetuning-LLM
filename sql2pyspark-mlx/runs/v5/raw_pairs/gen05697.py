from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("customer_id").orderBy("start_date", "id")
w_grp = Window.partitionBy("plan").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("plan").orderBy("is_enterprise", "id")

enriched = (
    df_subscriptions
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        F.lead("monthly_fee").over(w_recent).alias("next_monthly_fee"),
        F.lag("monthly_fee").over(w_recent).alias("prev_monthly_fee"),
        F.row_number().over(w_recent).alias("row_seq"),
        F.last("monthly_fee", ignorenulls=True).over(w_grp).alias("last_known_monthly_fee"),
    )
)

filtered = (
    enriched
    .filter((F.col("end_date") >= F.lit("2023-07-02").cast("date")) & (F.col("plan") == "enterprise"))
    .withColumn("end_date_year", F.year("end_date"))
    .withColumn("is_enterprise", F.when(F.col("plan") == "enterprise", 1).otherwise(0))
)

filtered_subscriptions = (
    filtered
    .filter((F.col("end_date") > F.to_date(F.lit("2023-09-18"))) & F.col("next_monthly_fee").isNotNull())
    .select(
        "*",
        F.ceil("last_known_monthly_fee").alias("last_known_monthly_fee_whole"),
        F.when(F.col("plan") == "basic", 1).otherwise(0).alias("is_basic"),
    )
)

latest = (
    filtered_subscriptions
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    latest
    .select(
        "id",
        "end_date",
        "next_monthly_fee",
        "prev_monthly_fee",
        "row_seq",
        "last_known_monthly_fee",
        "end_date_year",
        "is_enterprise",
        "last_known_monthly_fee_whole",
    )
    .orderBy(F.col("is_enterprise").desc_nulls_first())
)
