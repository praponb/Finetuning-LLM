from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name").orderBy(F.col("budget").desc(), "id")
w2 = Window.partitionBy("dept_name").orderBy(F.col("budget_adjusted").asc_nulls_first(), "id")
w3 = Window.partitionBy("dept_name").orderBy("budget_adjusted", "id").rowsBetween(-5, -1)
w4 = Window.partitionBy("dept_name").orderBy(F.col("budget_adjusted").desc(), "id")
w5 = Window.partitionBy("dept_name")
w6 = Window.orderBy("budget_adjusted", "id").rowsBetween(-2, Window.currentRow)
w7 = Window.partitionBy("dept_name").orderBy("budget_adjusted", "id").rowsBetween(-3, -1)

filtered = (
    df_departments
    .filter(F.col("location").isNotNull() & F.col("dept_name").isin(["Engineering", "Finance", "Marketing"]))
    .withColumn("dept_name_location", F.concat("dept_name", F.lit(" "), "location"))
    .withColumn("budget_adjusted", F.round((F.col("budget") * 1.07), 2))
    .withColumn("location_clean", F.coalesce("location", F.lit("NONE")))
)

ranked = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

ranked_departments = (
    ranked
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
    .select("id", "dept_name", "budget_adjusted", "location_clean")
)

with_windows = (
    ranked_departments
    .filter(F.col("dept_name").isin(["HR", "Engineering"]))
    .select(
        "id",
        "budget_adjusted",
        "location_clean",
        F.avg("budget_adjusted").over(w3).alias("budget_adjusted_avg_5_prior"),
        F.row_number().over(w4).alias("row_seq"),
        F.avg("budget_adjusted").over(w5).alias("dept_name_avg_budget_adjusted"),
        F.avg("budget_adjusted").over(w6).alias("budget_adjusted_avg_2"),
        F.avg("budget_adjusted").over(w7).alias("budget_adjusted_avg_3_prior"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "budget_adjusted",
        "location_clean",
        "row_seq",
        "dept_name_avg_budget_adjusted",
    )
)
