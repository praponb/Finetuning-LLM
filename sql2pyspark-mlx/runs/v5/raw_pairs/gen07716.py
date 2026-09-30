from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id")
w2 = Window.partitionBy("dept_name", "location").orderBy("budget", "id")

enriched = (
    df_departments
    .filter(F.col("budget") <= 3153000)
    .withColumn("prev_budget", F.lag("budget").over(w))
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("prev_budget_2", F.lag("budget", 1).over(w2))
)

result = (
    enriched
    .groupBy(
        F.when(F.col("prev_budget_2") >= 3951000, "PREMIUM").otherwise("STANDARD").alias("prev_budget_2_tier"),
        F.when(F.col("seq_no") >= 8, "LARGE").otherwise("SMALL").alias("seq_no_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("location")).alias("location_list"),
        F.sum(F.when(F.col("location") == "London", F.col("seq_no"))).alias("london_total_seq_no"),
    )
)
