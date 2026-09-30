from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy(F.col("opened_date_day").asc_nulls_last())
w2 = Window.partitionBy("region")
w3 = Window.orderBy(F.col("opened_date_day").desc_nulls_last())
w4 = Window.partitionBy("region").orderBy(F.col("opened_date_day_rank").desc(), "id")
w_part = Window.partitionBy("region", "city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_stores
    .filter(F.col("region").isNotNull() & (F.col("sqft") > 9261))
    .withColumn("opened_date_day", F.dayofmonth("opened_date"))
)

with_windows = (
    valid
    .filter(F.col("sqft").between(5584, 13354))
    .select(
        "*",
        F.rank().over(w).alias("opened_date_day_rank"),
        F.round((F.col("sqft") / F.sum("sqft").over(w2)), 4).alias("sqft_share"),
        F.percent_rank().over(w3).alias("opened_date_day_percent_rank"),
    )
)

filtered = (
    with_windows
    .filter(F.col("opened_date") > F.lit("2023-02-14").cast("date"))
    .select("*", F.col("sqft").cast("string").alias("sqft_num"))
)

latest = (
    filtered
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_windows_stores = (
    latest
    .select(
        "*",
        F.count("opened_date_day_percent_rank").over(w_part).alias("running_count_opened_date_day_percent_rank"),
        F.min("opened_date_day_percent_rank").over(w_part).alias("running_min_opened_date_day_percent_rank"),
        F.avg("opened_date_day_percent_rank").over(w_part).alias("running_avg_opened_date_day_percent_rank"),
    )
)

result = (
    with_windows_stores
    .select(
        "id",
        "opened_date",
        "opened_date_day",
        "opened_date_day_rank",
        "sqft_share",
        "opened_date_day_percent_rank",
        "sqft_num",
        "running_avg_opened_date_day_percent_rank",
    )
    .orderBy(F.col("opened_date_day_percent_rank").asc_nulls_first())
)
