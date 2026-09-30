from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "manager_id").orderBy(F.col("salary").desc(), "id")

latest = (
    df_employees
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest
    .groupBy(
        F.when(F.col("salary") >= 149000, "HIGH").otherwise("LOW").alias("salary_tier"),
    )
    .agg(
        F.percentile_approx("salary", 0.5).alias("salary_p50"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.percentile_approx("salary", 0.5).alias("salary_p50_2"),
        F.sum(F.when(F.col("job_title") == "Designer", F.col("salary")).otherwise(0)).alias("designer_salary"),
    )
    .filter(F.col("salary_p50_2") > 0)
)
