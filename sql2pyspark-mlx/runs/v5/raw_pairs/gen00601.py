from pyspark.sql import functions as F

base = (
    df_subscriptions
    .filter((F.col("end_date") > F.lit("2023-10-09").cast("date")) & (F.col("plan") == "premium"))
    .select(
        "*",
        F.when(F.col("status") == "PAUSED", 1).otherwise(0).alias("is_paused"),
        F.add_months("start_date", 1).alias("start_date_plus_months"),
    )
)

result = (
    base
    .select("id", "status")
    .intersect(
        df_subscriptions
        .filter(F.col("end_date") > "2023-08-09")
        .select("id", "status")
    )
)
