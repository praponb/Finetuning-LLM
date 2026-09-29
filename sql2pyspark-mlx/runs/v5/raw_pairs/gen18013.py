from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("end_date").desc_nulls_last(), "id")

cleaned = (
    df_subscriptions
    .filter((F.col("end_date") > F.to_date(F.lit("2023-08-11"))) & F.col("customer_id").isNotNull())
    .withColumn("start_date_month_end", F.last_day("start_date"))
    .withColumn("monthly_fee_level", F.when(F.col("monthly_fee") >= 184, "High").when(F.col("monthly_fee") >= 89, "Medium").otherwise("Low"))
)

result = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)
