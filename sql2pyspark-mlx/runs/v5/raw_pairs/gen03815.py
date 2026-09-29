from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id", "job_title").orderBy(F.col("is_l2_adjusted").desc_nulls_first(), "id")
w2 = Window.partitionBy("dept_id").orderBy("hire_date_plus_months", "id").rowsBetween(-6, Window.currentRow)

base = (
    df_employees
    .filter((~F.col("level").eqNullSafe(F.lit("L1"))) & (F.col("hire_date") > F.lit("2023-05-24").cast("date")))
    .select("*", F.when(F.col("level") == "L2", 1).otherwise(0).alias("is_l2"))
)

valid = (
    base
    .filter(F.col("manager_id").isNotNull())
    .select(
        "id",
        "dept_id",
        "job_title",
        "hire_date",
        "is_remote",
        "level",
        "is_l2",
        F.concat("job_title", F.lit(" - "), "level").alias("job_title_level"),
        F.when(F.col("is_l2") >= 61, "High").when(F.col("is_l2") >= 39, "Medium").otherwise("Low").alias("is_l2_level"),
    )
)

cleaned = (
    valid
    .filter(F.col("hire_date") > F.lit("2023-05-11").cast("date"))
    .select(
        "*",
        F.round((F.col("is_l2") * 1.07), 2).alias("is_l2_adjusted"),
        F.add_months("hire_date", 3).alias("hire_date_plus_months"),
    )
)

latest = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

scored = (
    latest
    .select(
        "*",
        F.count("is_l2_adjusted").over(w2).alias("last7_count_is_l2_adjusted"),
        F.sum("is_l2_adjusted").over(w2).alias("last7_sum_is_l2_adjusted"),
        F.max("is_l2_adjusted").over(w2).alias("last7_max_is_l2_adjusted"),
    )
)

with_dims = (
    scored
    .alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.location"), F.col("d.budget"))
)

result = (
    with_dims
    .select(
        "id",
        "dept_id",
        F.col("last7_sum_is_l2_adjusted").alias("LAST7_SUM_IS_L2_ADJUSTED_VAL"),
        "is_l2_adjusted",
        F.col("budget").alias("BUDGET_VAL"),
    )
    .unpivot(
        ids=["id", "dept_id"],
        values=["LAST7_SUM_IS_L2_ADJUSTED_VAL", "is_l2_adjusted", "BUDGET_VAL"],
        variableColumnName="kpi",
        valueColumnName="reading",
    )
)
