from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("end_date").desc_nulls_last(), "id")
w2 = Window.partitionBy("plan").orderBy(F.col("start_date").desc_nulls_last(), "id")
w3 = Window.partitionBy("plan").orderBy(F.col("days_since_start_date").desc_nulls_last(), "id")

deduped = (
    df_subscriptions
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "plan", "start_date")
)

valid = (
    deduped
    .filter(F.col("start_date") >= F.lit("2023-03-11").cast("date"))
    .withColumn("days_since_start_date", F.datediff(F.current_date(), "start_date"))
    .withColumn("plan_title", F.initcap(F.trim("plan")))
)

ranked = (
    valid
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

latest = (
    ranked
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "plan", "start_date", "days_since_start_date", "plan_title")
)

base = (
    latest
    .filter(F.col("start_date") < "2024-04-17")
    .select(
        "id",
        "plan",
        "days_since_start_date",
        "plan_title",
        F.when(F.col("plan") == "standard", 1).otherwise(0).alias("is_standard"),
    )
)

result = base.select("id", "plan", "days_since_start_date", "plan_title", "is_standard")
