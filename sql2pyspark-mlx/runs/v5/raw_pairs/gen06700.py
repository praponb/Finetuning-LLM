from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name").orderBy(F.col("budget").desc_nulls_last(), "id")

ranked = (
    df_departments
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .groupBy(
        F.when(F.col("budget") >= 4039000, "HIGH").otherwise("LOW").alias("budget_tier"),
    )
    .agg(
        F.size(F.collect_set("location")).alias("num_locations"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
    )
    .filter(F.col("num_locations") < 1000)
    .orderBy("budget_tier")
)
