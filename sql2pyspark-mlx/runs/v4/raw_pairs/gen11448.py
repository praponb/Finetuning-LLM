from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("budget", "id")
w2 = Window.orderBy(F.col("budget").desc(), "id")
w3 = Window.partitionBy("location").orderBy("budget", "id")
w4 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("dept_name").orderBy("budget", "id")
w6 = Window.partitionBy("dept_name", "location").orderBy(F.col("event_seq").desc_nulls_first(), "id")

scored = (
    df_departments
    .select(
        "*",
        F.lead("budget", 1).over(w).alias("next_budget"),
        F.ntile(3).over(w2).alias("budget_bucket"),
        F.row_number().over(w3).alias("seq_no"),
        F.sum("budget").over(w4).alias("cumulative_budget"),
        F.row_number().over(w_ordered).alias("event_seq"),
    )
)

deduped = (
    scored
    .withColumn("row_num", F.row_number().over(w6))
    .filter(F.col("row_num") == 1)
)

filtered = (
    deduped
    .filter(F.col("dept_name").isin(["Marketing", "Sales"]))
    .withColumn("event_seq_tier", F.when(F.col("event_seq") >= 7, "High").when(F.col("event_seq") >= 3, "Medium").otherwise("Low"))
)

eligible = (
    filtered
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.is_remote")), "left_anti")
)

kept_keys = (
    eligible
    .select("location", "dept_name")
    .intersect(
        df_departments
        .filter(F.col("dept_name").isNull())
        .select("location", "dept_name")
    )
)

reconciled = (
    eligible
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.location") == F.col("k.location")) & (F.col("s.dept_name") == F.col("k.dept_name")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "location",
        "budget",
        "next_budget",
        "budget_bucket",
        "seq_no",
        "cumulative_budget",
        "event_seq",
        "row_num",
    )
)
