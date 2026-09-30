from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("manager_id").orderBy("hire_date", "id")
w2 = Window.orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("level").orderBy(F.col("row_seq").asc_nulls_last(), "id")
w4 = Window.partitionBy("level").orderBy(F.col("row_seq").desc(), "id")
w5 = Window.partitionBy("level").orderBy(F.col("next_salary").asc_nulls_first(), "id")

joined = (
    df_employees
    .alias("e")
    .join(df_employees.alias("p"), F.col("e.manager_id") == F.col("p.id"), "inner")
    .select(
        "e.*",
        F.col("p.salary").alias("employee_salary"),
        F.col("p.level").alias("employee_level"),
    )
)

enriched = (
    joined
    .select(
        "id",
        "full_name",
        "email",
        "level",
        F.lag("salary").over(w_recent).alias("prev_salary"),
        F.lead("salary").over(w_recent).alias("next_salary"),
        F.lag("employee_salary").over(w_recent).alias("prev_employee_salary"),
        F.lead("employee_salary").over(w_recent).alias("next_employee_salary"),
        F.row_number().over(w_recent).alias("row_seq"),
        F.sum("salary").over(w2).alias("cumulative_salary"),
    )
)

latest = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "full_name", "level", "next_salary", "row_seq", "cumulative_salary")
)

deduped = (
    latest
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "level", "next_salary", "cumulative_salary")
)

ranked = (
    deduped
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .select("id", "level", "next_salary", "cumulative_salary", "row_num")
    .orderBy(F.col("level").desc())
)
