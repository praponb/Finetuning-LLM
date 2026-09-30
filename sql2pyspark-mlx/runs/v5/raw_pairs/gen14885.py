from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name").orderBy(F.col("budget").desc(), "id")
w2 = Window.partitionBy("location").orderBy(F.col("budget").desc_nulls_last())
w3 = Window.partitionBy("location").orderBy(F.col("budget_scaled").desc(), "id")
w4 = Window.orderBy("budget_scaled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("dept_name", "location")
w6 = Window.partitionBy("location", "dept_name").orderBy("budget", "id")
w7 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

latest = (
    df_departments
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

ranked = (
    latest
    .withColumn("rank_in_group", F.rank().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

not_flagged = (
    ranked
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.salary").between(118000, 143000)), "left_anti")
)

valid = (
    not_flagged
    .filter(F.col("location") != "Chiang Mai")
    .withColumn("budget_scaled", F.round((F.col("budget") * 1.1), 2))
    .withColumn("budget_tier", F.when(F.col("budget") >= 4180000, "High").when(F.col("budget") >= 1427000, "Medium").otherwise("Low"))
)

deduped = (
    valid
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

scored = (
    deduped
    .select(
        "id",
        "dept_name",
        "budget_scaled",
        "budget_tier",
        F.first("dept_name", ignorenulls=True).over(w4).alias("first_known_dept_name"),
        F.count("budget").over(w5).alias("dept_name_count_budget"),
        F.row_number().over(w6).alias("event_seq"),
        F.last("budget", ignorenulls=True).over(w7).alias("last_known_budget"),
    )
)

base = (
    scored
    .filter(F.col("dept_name_count_budget") != 8)
    .withColumn("event_seq_minus_dept_name_count_budget", (F.col("event_seq") - F.col("dept_name_count_budget")))
)

result = (
    base
    .groupBy("dept_name")
    .agg(
        F.sum(F.when(F.col("dept_name") == "Marketing", F.col("dept_name_count_budget"))).alias("marketing_total_dept_name_count_budget"),
        F.count(F.when(F.col("dept_name") == "Finance", 1)).alias("finance_count"),
        F.count(F.when(F.col("dept_name") == "Finance", 1)).alias("finance_rows"),
        F.count("*").alias("cnt"),
        F.round(F.avg("budget_scaled"), 2).alias("avg_budget_scaled"),
        F.sum(F.when(F.col("dept_name") == "Sales", F.col("dept_name_count_budget")).otherwise(0)).alias("sales_dept_name_count_budget"),
    )
    .filter(F.col("finance_rows") > 1)
    .orderBy(F.col("sales_dept_name_count_budget").desc_nulls_first())
)
