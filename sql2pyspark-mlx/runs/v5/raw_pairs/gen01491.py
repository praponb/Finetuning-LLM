from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_employees
    .filter(F.col("dept_id").isin([7, 18, 27]))
    .select("*", F.round((F.col("salary") * 0.9), 2).alias("adjusted_salary"))
)

result = (
    base
    .select(
        "*",
        F.min("adjusted_salary").over(w).alias("running_min_adjusted_salary"),
        F.avg("adjusted_salary").over(w).alias("running_avg_adjusted_salary"),
        F.count("adjusted_salary").over(w).alias("running_count_adjusted_salary"),
    )
)
