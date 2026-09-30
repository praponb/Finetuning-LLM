from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("location").orderBy("budget", "id")
w2 = Window.orderBy(F.col("budget").desc())

enriched = (
    df_departments
    .select(
        "id",
        "dept_name",
        F.lag("budget").over(w_recent).alias("prev_budget"),
        F.row_number().over(w_recent).alias("event_seq"),
        F.rank().over(w2).alias("budget_rank"),
    )
)

result = (
    enriched
    .select("id", "budget_rank")
    .orderBy(F.col("budget_rank").asc_nulls_first())
)
