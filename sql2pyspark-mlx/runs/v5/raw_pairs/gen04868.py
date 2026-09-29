from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("days_since_opened_date", "id").rowsBetween(-6, -1)
w_recent = Window.partitionBy("region", "city").orderBy(F.col("days_since_opened_date_2").desc_nulls_last())
w3 = Window.partitionBy("city", "region").orderBy("days_since_opened_date_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("days_since_opened_date_2").desc_nulls_first())

cleaned = (
    df_stores
    .filter(F.col("region").isNotNull() & F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft",
        F.datediff(F.current_date(), "opened_date").alias("days_since_opened_date"),
        F.datediff(F.current_date(), "opened_date").alias("days_since_opened_date_2"),
        F.coalesce("region", F.lit("UNKNOWN")).alias("region_filled"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "store_name",
        "region",
        "sqft",
        "days_since_opened_date",
        "region_filled",
        F.avg("days_since_opened_date").over(w).alias("days_since_opened_date_avg_6_prior"),
        F.percent_rank().over(w_recent).alias("days_since_opened_date_2_percent_rank"),
        F.last("days_since_opened_date", ignorenulls=True).over(w3).alias("last_known_days_since_opened_date"),
        F.rank().over(w4).alias("days_since_opened_date_2_rank"),
    )
)
