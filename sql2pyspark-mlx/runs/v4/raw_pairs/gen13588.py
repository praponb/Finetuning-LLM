from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("budget", "id").rowsBetween(-6, -1)

enriched = (
    df_departments
    .withColumn("cumulative_budget", F.sum("budget").over(w))
    .withColumn("budget_avg_6_prior", F.avg("budget").over(w2))
)

result = (
    enriched
    .groupBy("location")
    .agg(
        F.count(F.when(F.col("dept_name") == "HR", 1)).alias("hr_count"),
        F.count_if(F.col("dept_name") == "Engineering").alias("engineering_events"),
        F.count("*").alias("event_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count_if(F.col("dept_name") == "Legal").alias("legal_events"),
        F.sum(F.when(F.col("location") == "Chiang Mai", F.col("budget"))).alias("chiang_mai_total_budget"),
    )
    .orderBy(F.col("hr_count").desc_nulls_last())
)
