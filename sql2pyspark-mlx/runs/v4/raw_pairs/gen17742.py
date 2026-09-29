from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "manager_id").orderBy(F.col("hire_date").desc(), "id")

ranked = (
    df_employees
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .withColumn("salary_tier", F.when(F.col("salary") >= 127000, "HIGH").otherwise("LOW"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "hire_date"))
    .cube("salary_tier", "quarter_start")
    .agg(
        F.stddev_samp("salary").alias("salary_stddev"),
        F.percentile_approx("salary", 0.5).alias("salary_p50"),
        F.count(F.when(F.col("job_title") == "Manager", 1)).alias("manager_count"),
        F.grouping("salary_tier").alias("grouping_level"),
    )
)
