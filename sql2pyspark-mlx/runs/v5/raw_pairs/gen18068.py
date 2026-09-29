from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "customer_id").orderBy(F.col("start_date").desc_nulls_first())
w2 = Window.partitionBy("customer_id").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("plan").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy("start_date", "id")
w5 = Window.orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("status").orderBy("row_num", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("plan").orderBy("row_num", "id")

latest = (
    df_subscriptions
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

with_windows = (
    latest
    .filter(F.col("end_date").between("2024-09-24", "2024-12-15"))
    .select(
        "*",
        F.first("monthly_fee", ignorenulls=True).over(w2).alias("first_known_monthly_fee"),
        F.first("monthly_fee", ignorenulls=True).over(w3).alias("first_known_monthly_fee_2"),
        F.lag("monthly_fee", 1).over(w4).alias("prev_monthly_fee"),
        F.first("monthly_fee", ignorenulls=True).over(w5).alias("first_known_monthly_fee_3"),
    )
)

base = (
    with_windows
    .filter((F.col("start_date") > F.lit("2023-05-14").cast("date")) & F.col("customer_id").isNotNull())
    .withColumn("days_since_start_date", F.datediff(F.current_date(), "start_date"))
    .withColumn("first_known_monthly_fee_3_filled", F.coalesce("first_known_monthly_fee_3", F.lit(0)))
)

enriched = (
    base
    .withColumn("running_first_known_monthly_fee_3_filled", F.sum("first_known_monthly_fee_3_filled").over(w6))
    .withColumn("next_days_since_start_date", F.lead("days_since_start_date", 1).over(w7))
)

matched_keys = (
    enriched
    .select("status", "customer_id")
    .subtract(
        df_subscriptions
        .filter(F.col("end_date") < "2025-01-01")
        .select("status", "customer_id")
    )
)

verified = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "row_num",
        "prev_monthly_fee",
        "first_known_monthly_fee_3",
        F.when(F.col("plan").rlike("^(basic|premium)$"), "GROUP_B").when(F.col("plan").rlike("^(standard|enterprise)$"), "PRIORITY").otherwise("GROUP_A").alias("plan_zone"),
    )
    .orderBy(F.col("customer_id").desc_nulls_last())
)
