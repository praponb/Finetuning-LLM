from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name").orderBy(F.col("budget").desc())
w_ordered = Window.partitionBy("location").orderBy("rn", "id")
w3 = Window.partitionBy("dept_name").orderBy("rn", "id")

latest = (
    df_departments
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

with_windows = (
    latest
    .select(
        "id",
        "dept_name",
        "budget",
        "rn",
        F.lag("budget").over(w_ordered).alias("prev_budget"),
        F.row_number().over(w_ordered).alias("seq_no"),
        F.row_number().over(w3).alias("row_seq"),
    )
)

base = (
    with_windows
    .filter(F.col("row_seq") <= 8)
    .select(
        "id",
        "budget",
        "prev_budget",
        "seq_no",
        "row_seq",
        F.least("row_seq", "seq_no").alias("least_row_seq_seq_no"),
        F.col("prev_budget").cast("int").alias("prev_budget_int"),
    )
)

filtered = (
    base
    .filter(F.col("least_row_seq_seq_no") < 52)
    .select(
        "id",
        "budget",
        "prev_budget",
        "seq_no",
        (F.col("row_seq") + F.col("least_row_seq_seq_no")).alias("row_seq_plus_least_row_seq_seq_no"),
        F.coalesce("prev_budget_int", F.lit(0)).alias("prev_budget_int_filled"),
    )
)

not_flagged = (
    filtered
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.salary") <= 65000), "left_semi")
)

result = not_flagged.select("id", "seq_no")
