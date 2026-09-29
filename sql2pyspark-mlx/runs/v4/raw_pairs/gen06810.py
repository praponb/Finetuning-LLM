from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id")
w2 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("budget", "id")
w4 = Window.partitionBy("dept_name")

with_windows = (
    df_departments
    .withColumn("budget_bucket", F.ntile(3).over(w))
    .withColumn("next_budget", F.lead("budget", 1).over(w))
    .withColumn("first_known_location", F.first("location", ignorenulls=True).over(w2))
    .withColumn("budget_bucket_2", F.ntile(3).over(w3))
    .withColumn("dept_name_count_budget", F.count("budget").over(w4))
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("budget_bucket") >= 7, "PREMIUM").otherwise("STANDARD").alias("budget_bucket_tier"),
    )
    .agg(
        F.stddev_samp("budget").alias("budget_stddev"),
        F.size(F.collect_set("location")).alias("num_locations"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
    )
    .orderBy(F.col("num_locations").desc_nulls_first())
)
