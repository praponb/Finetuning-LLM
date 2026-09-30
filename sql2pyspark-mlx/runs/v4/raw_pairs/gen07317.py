from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id", "level").orderBy("job_title_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("salary").desc(), "id")
w3 = Window.partitionBy("manager_id").orderBy(F.col("job_title_length").desc_nulls_first())
w4 = Window.partitionBy("level", "manager_id").orderBy("job_title_length", "id").rowsBetween(-6, Window.currentRow)
w5 = Window.partitionBy("level").orderBy("job_title_length_cume_dist", "id").rowsBetween(-6, -1)
w6 = Window.partitionBy("level").orderBy(F.col("employee_salary").desc_nulls_first())
w7 = Window.partitionBy("level", "manager_id").orderBy(F.col("cumulative_job_title_length").asc_nulls_last())

filtered = (
    df_employees
    .filter(F.col("dept_id").isin([4, 31]))
    .select(
        "id",
        "full_name",
        "manager_id",
        "salary",
        "level",
        F.when(F.col("job_title") == "Manager", 1).otherwise(0).alias("is_manager"),
        F.length("job_title").alias("job_title_length"),
    )
)

enriched = (
    filtered
    .select(
        "*",
        F.sum("job_title_length").over(w).alias("cumulative_job_title_length"),
        F.ntile(4).over(w2).alias("salary_bucket"),
        F.cume_dist().over(w3).alias("job_title_length_cume_dist"),
    )
)

joined = (
    enriched
    .alias("a")
    .join(df_employees.alias("p"), F.col("a.manager_id") == F.col("p.id"), "left")
    .select(
        "a.*",
        F.col("p.salary").alias("employee_salary"),
        F.col("p.email"),
        F.col("p.job_title"),
    )
)

metrics = (
    joined
    .select(
        "*",
        F.avg("salary").over(w4).alias("salary_avg_6"),
        F.avg("salary_bucket").over(w5).alias("salary_bucket_avg_6_prior"),
        F.dense_rank().over(w6).alias("employee_salary_dense_rank"),
        F.dense_rank().over(w7).alias("cumulative_job_title_length_dense_rank"),
    )
)

by_type = (
    metrics
    .groupBy("level")
    .pivot("job_title", ["Engineer", "Analyst"])
    .agg(F.max("salary_bucket").alias("total"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "level",
        F.col("Engineer_total").alias("engineer_total"),
        F.col("Engineer_cnt").alias("engineer_cnt"),
        F.col("Analyst_total").alias("analyst_total"),
        F.col("Analyst_cnt").alias("analyst_cnt"),
    )
)

result = (
    by_type
    .select(
        "level",
        "analyst_total",
        "analyst_cnt",
        F.when(F.col("level").rlike("^(L5|L1)$"), "CORE").otherwise("GROUP_A").alias("level_zone"),
    )
    .orderBy("analyst_cnt", "level_zone")
)
