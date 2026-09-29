from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id")

result = (
    df_departments
    .select(
        "id",
        "dept_name",
        F.lag("budget").over(w).alias("prev_budget"),
        F.lead("budget").over(w).alias("next_budget"),
        F.row_number().over(w).alias("event_seq"),
    )
)
