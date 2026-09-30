from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "plan").orderBy(F.col("start_date").desc(), "id")

cleaned = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(80, 160))
    .withColumn("status_lower", F.lower("status"))
)

valid = (
    cleaned
    .filter((F.col("end_date") >= F.lit("2023-10-26").cast("date")) & (F.col("monthly_fee") <= 70))
    .select(
        "id",
        "plan",
        "start_date",
        "monthly_fee",
        "status",
        F.date_trunc("week", "end_date").alias("end_date_period"),
    )
)

ranked = (
    valid
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

kept_keys = (
    ranked
    .select("status", "id")
    .intersect(
        df_subscriptions
        .filter(F.col("status") == "PAUSED")
        .select("status", "id")
    )
)

reconciled = (
    ranked
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = reconciled.select("start_date", "status", "end_date_period")
