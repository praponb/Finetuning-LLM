from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("salary", "id")
w2 = Window.partitionBy("level", "job_title").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("level", "manager_id").orderBy(F.col("salary").desc())
w4 = Window.partitionBy("level").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("job_title").orderBy("salary", "id").rowsBetween(-6, Window.currentRow)
w6 = Window.partitionBy("level").orderBy(F.col("salary_dense_rank").desc(), "id")
w7 = Window.partitionBy("job_title").orderBy(F.col("salary_dense_rank").desc(), "id")
w8 = Window.partitionBy("level").orderBy("salary", "id")

valid = (
    df_employees
    .filter((~F.col("job_title").eqNullSafe(F.lit("Director"))) & (F.col("level") != "L4"))
    .select("*", F.lower("job_title").alias("job_title_lower"))
)

enriched = (
    valid
    .filter(F.col("level").isNull())
    .select(
        "id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.lead("salary", 1).over(w).alias("next_salary"),
        F.sum("salary").over(w2).alias("running_salary"),
        F.dense_rank().over(w3).alias("salary_dense_rank"),
        F.last("job_title", ignorenulls=True).over(w4).alias("last_known_job_title"),
    )
)

enriched_employees = (
    enriched
    .withColumn("salary_avg_6", F.avg("salary").over(w_grp))
    .withColumn("salary_dense_rank_bucket", F.ntile(5).over(w6))
)

deduped = (
    enriched_employees
    .withColumn("rank_in_group", F.row_number().over(w7))
    .filter(F.col("rank_in_group") == 1)
)

ranked = (
    deduped
    .withColumn("row_num", F.row_number().over(w8))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .groupBy(
        "job_title",
        F.when(F.col("running_salary") >= 641421, "LARGE").otherwise("SMALL").alias("running_salary_tier"),
    )
    .agg(
        F.round(F.avg("salary_dense_rank"), 2).alias("avg_salary_dense_rank"),
        F.round(F.avg("running_salary"), 2).alias("avg_running_salary"),
        F.avg(F.when(F.col("level") == "L2", F.col("salary_dense_rank"))).alias("l2_avg_salary_dense_rank"),
    )
    .orderBy(F.col("avg_running_salary").desc())
)
