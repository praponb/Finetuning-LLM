from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("start_date", "id").rowsBetween(-6, Window.currentRow)
w_recent = Window.partitionBy("plan").orderBy("start_date_year", "id")

filtered = (
    df_subscriptions
    .filter((F.col("start_date") > F.lit("2023-03-11").cast("date")) & (F.col("plan") != "standard"))
    .select("*", F.year("start_date").alias("start_date_year"))
)

result = (
    filtered
    .filter(F.col("customer_id").isin([3, 20, 27, 35]))
    .select(
        "id",
        "plan",
        "start_date",
        "monthly_fee",
        "start_date_year",
        F.avg("start_date_year").over(w).alias("start_date_year_avg_6"),
        F.lead("start_date_year", 1).over(w_recent).alias("next_start_date_year"),
    )
)
