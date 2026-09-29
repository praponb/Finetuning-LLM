from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment")
w2 = Window.partitionBy("country").orderBy("credit_limit", "id").rowsBetween(-6, Window.currentRow)
w_running = Window.orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("city", "segment")

scored = (
    df_customers
    .select(
        "id",
        "name",
        "country",
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        F.avg("credit_limit").over(w).alias("segment_avg_credit_limit"),
        F.avg("credit_limit").over(w2).alias("credit_limit_avg_6"),
        F.last("credit_limit", ignorenulls=True).over(w_running).alias("last_known_credit_limit"),
        F.avg("credit_limit").over(w4).alias("city_avg_credit_limit"),
        F.round((F.col("age") / F.sum("age").over(w)), 4).alias("age_share"),
    )
)

result = (
    scored
    .groupBy("segment")
    .agg(
        F.countDistinct("city").alias("distinct_cities"),
        F.count_if(F.col("city") == "Bangkok").alias("bangkok_events"),
    )
    .orderBy(F.col("bangkok_events").asc_nulls_last())
)
