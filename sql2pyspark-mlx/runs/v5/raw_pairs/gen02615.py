from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location").orderBy("budget", "id")

ranked = (
    df_departments
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .groupBy(
        F.when(F.col("budget") >= 4488000, "HIGH").otherwise("LOW").alias("budget_tier"),
        "dept_name",
    )
    .agg(
        F.sum(F.when(F.col("location") == "Tokyo", F.col("budget")).otherwise(0)).alias("tokyo_budget"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
        F.count("*").alias("cnt"),
        F.round(F.sum("budget") / F.nullif(F.count(F.when(F.col("dept_name") == "Finance", 1)), F.lit(0)), 2).alias("budget_per_finance"),
        F.count("*").alias("cnt_2"),
        F.stddev_samp("budget").alias("budget_stddev"),
    )
    .filter(F.col("tokyo_budget") < 1000000)
    .orderBy(F.col("budget_per_finance").desc())
)
