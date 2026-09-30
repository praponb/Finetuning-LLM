from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("level").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.orderBy(F.col("salary").desc())
w_part_2 = Window.partitionBy("job_title").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("manager_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("level")

enriched = (
    df_employees
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "salary",
        "hire_date",
        F.first("level", ignorenulls=True).over(w_part).alias("first_known_level"),
        F.dense_rank().over(w_ordered).alias("salary_dense_rank"),
        F.sum("salary").over(w_part_2).alias("cumulative_salary"),
        F.sum("salary").over(w4).alias("cumulative_salary_2"),
        F.round((F.col("salary") / F.sum("salary").over(w5)), 4).alias("salary_share"),
    )
)

result = (
    enriched
    .groupBy(F.date_trunc("MONTH", "hire_date").alias("activity_month"))
    .agg(
        F.percentile_approx("salary", [0.25, 0.5, 0.75]).alias("salary_quartiles"),
        F.count("*").alias("event_count"),
    )
    .orderBy(F.col("event_count").desc_nulls_first())
)
