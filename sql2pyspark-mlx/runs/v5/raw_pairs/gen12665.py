from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("salary", "id")
w2 = Window.partitionBy("job_title").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("level").orderBy("is_l1", "id")

filtered = (
    df_employees
    .filter((F.col("hire_date") >= F.to_date(F.lit("2023-02-12"))) & F.col("level").isNotNull())
    .select(
        "id",
        "email",
        "dept_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.when(F.col("level") == "L1", 1).otherwise(0).alias("is_l1"),
        F.coalesce("level", F.lit("UNKNOWN")).alias("level_filled"),
    )
)

result = (
    filtered
    .select(
        "id",
        "email",
        "dept_id",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        "is_l1",
        F.row_number().over(w).alias("event_seq"),
        F.last("level", ignorenulls=True).over(w2).alias("last_known_level"),
        F.sum("salary").over(w3).alias("running_salary"),
        F.lag("is_l1", 1).over(w4).alias("prev_is_l1"),
    )
)
