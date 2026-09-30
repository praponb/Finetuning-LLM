from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("level")
w4 = Window.partitionBy("manager_id").orderBy("salary", "id")

enriched = (
    df_employees
    .select(
        "*",
        F.sum("salary").over(w).alias("cumulative_salary"),
        F.sum("salary").over(w2).alias("running_salary"),
        F.stddev_samp("salary").over(w3).alias("level_stddev_samp_salary"),
        F.row_number().over(w4).alias("seq_no"),
    )
)

result = (
    enriched
    .groupBy("seq_no")
    .agg(
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.count(F.when(F.col("level") == "L2", 1)).alias("l2_count"),
        F.sort_array(F.collect_set("level")).alias("level_list_2"),
        F.size(F.collect_set("job_title")).alias("num_job_titles"),
    )
    .orderBy(F.col("seq_no").desc())
)
