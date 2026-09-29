from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("hire_date").desc_nulls_first(), "id")
w2 = Window.partitionBy("manager_id").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("manager_id", "level").orderBy("hire_date", "id")
w4 = Window.partitionBy("manager_id")
w5 = Window.partitionBy("level").orderBy("rn", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("level").orderBy(F.col("salary_share").desc())
w8 = Window.partitionBy("level")
w_part = Window.partitionBy("level").orderBy("cumulative_salary", "id")
w10 = Window.partitionBy("manager_id", "job_title").orderBy(F.col("next_salary").desc(), "id")

deduped = (
    df_employees
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

enriched = (
    deduped
    .select(
        "*",
        F.last("level", ignorenulls=True).over(w2).alias("last_known_level"),
        F.lead("salary", 2).over(w3).alias("next_salary"),
        F.round((F.col("salary") / F.sum("salary").over(w4)), 4).alias("salary_share"),
        F.sum("salary").over(w5).alias("cumulative_salary"),
        F.sum("salary").over(w6).alias("cumulative_salary_2"),
    )
)

enriched_employees = (
    enriched
    .withColumn("salary_share_rank", F.rank().over(w7))
    .withColumn("next_salary_share", F.round((F.col("next_salary") / F.sum("next_salary").over(w8)), 4))
    .withColumn("prev_cumulative_salary", F.lag("cumulative_salary", 1).over(w_part))
    .withColumn("prev_salary", F.lag("salary", 1).over(w_part))
    .withColumn("next_salary_bucket", F.ntile(4).over(w10))
)

result = (
    enriched_employees
    .select(
        "id",
        "job_title",
        "salary",
        "is_remote",
        "last_known_level",
        "next_salary",
        "salary_share",
        "cumulative_salary",
        "salary_share_rank",
    )
    .orderBy(F.col("salary_share_rank").desc())
)
