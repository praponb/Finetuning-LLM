from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title")
w2 = Window.partitionBy("dept_id").orderBy("salary", "id")
w3 = Window.partitionBy("job_title").orderBy(F.col("employee_salary_share").asc_nulls_last())

with_dims = (
    df_employees
    .alias("src")
    .join(df_departments.alias("d"), F.col("src.dept_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.location"), F.col("d.dept_name"), F.col("d.budget"))
)

joined = (
    with_dims
    .alias("s")
    .join(df_employees.alias("d"), F.col("s.manager_id") == F.col("d.id"), "left")
    .select(
        "s.*",
        F.col("d.email").alias("employee_email"),
        F.col("d.salary").alias("employee_salary"),
    )
)

enriched = (
    joined
    .select(
        "*",
        F.round((F.col("employee_salary") / F.sum("employee_salary").over(w)), 4).alias("employee_salary_share"),
        F.lead("salary", 2).over(w2).alias("next_salary"),
    )
)

deduped = (
    enriched
    .withColumn("rank_in_group", F.rank().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    deduped
    .groupBy("level", "location")
    .pivot("job_title", ["Designer", "Intern"])
    .agg(F.max("budget").alias("amount"), F.count(F.lit(1)).alias("item_count"))
    .select(
        "level",
        "location",
        F.col("Designer_amount").alias("designer_amount"),
        F.col("Designer_item_count").alias("designer_item_count"),
        F.col("Intern_amount").alias("intern_amount"),
        F.col("Intern_item_count").alias("intern_item_count"),
    )
)
