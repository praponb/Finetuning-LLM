from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc(), "id")
w2 = Window.partitionBy("location")
w3 = Window.partitionBy("location_avg_budget_tier").orderBy(F.col("legal_avg_location_avg_budget").desc())

enriched = (
    df_departments
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("location_avg_budget", F.avg("budget").over(w2))
)

result = (
    enriched
    .groupBy(
        F.when(F.col("location_avg_budget") >= 3705321, "HIGH").otherwise("LOW").alias("location_avg_budget_tier"),
        F.when(F.col("budget") >= 3394000, "HIGH").otherwise("LOW").alias("budget_tier"),
    )
    .agg(
        F.avg(F.when(F.col("dept_name") == "Legal", F.col("location_avg_budget"))).alias("legal_avg_location_avg_budget"),
        F.round(F.sum("location_avg_budget") / F.nullif(F.count(F.when(F.col("dept_name") == "Legal", 1)), F.lit(0)), 2).alias("location_avg_budget_per_legal"),
        F.size(F.collect_set("dept_name")).alias("num_dept_names"),
    )
    .filter(F.col("location_avg_budget_per_legal") >= 0)
    .withColumn("legal_avg_location_avg_budget_rank", F.rank().over(w3))
    .orderBy(F.col("budget_tier").desc())
)
