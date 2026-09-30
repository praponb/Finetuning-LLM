from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment", "country").orderBy(F.col("age").desc_nulls_last())

latest = (
    df_customers
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    latest
    .groupBy(
        "country",
        F.date_trunc("MONTH", "signup_date").alias("activity_month"),
        F.date_trunc("QUARTER", "signup_date").alias("quarter_start"),
    )
    .agg(
        F.count(F.when(F.col("segment") == "Corporate", 1)).alias("corporate_count"),
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
        F.count_if(F.col("country") == "JP").alias("jp_events"),
    )
    .filter(F.col("corporate_count") < 1)
)
