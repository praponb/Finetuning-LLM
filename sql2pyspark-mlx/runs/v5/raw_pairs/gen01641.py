from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy(F.col("salary").desc(), "id")
w2 = Window.orderBy("salary", "id").rowsBetween(-5, Window.currentRow)
w_recent = Window.partitionBy("manager_id").orderBy("salary_scaled", "id")
w4 = Window.partitionBy("manager_id")
w5 = Window.orderBy("prev_salary_scaled", "id")
w6 = Window.orderBy(F.col("salary_avg_5").asc_nulls_first())
w7 = Window.orderBy(F.col("salary").desc(), "id")
w8 = Window.orderBy("salary_avg_5", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy(F.col("manager_id_min_salary_scaled").desc())

latest = (
    df_employees
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "manager_id", "salary", "is_remote")
)

cleaned = (
    latest
    .filter(F.col("is_remote"))
    .select("*", F.round((F.col("salary") * 1.1), 2).alias("salary_scaled"))
)

with_windows = (
    cleaned
    .select(
        "id",
        "salary",
        "salary_scaled",
        F.avg("salary").over(w2).alias("salary_avg_5"),
        F.lag("salary_scaled", 1).over(w_recent).alias("prev_salary_scaled"),
        F.min("salary_scaled").over(w4).alias("manager_id_min_salary_scaled"),
    )
)

base = (
    with_windows
    .filter(F.col("manager_id_min_salary_scaled") == 23)
    .select(
        "id",
        "salary",
        "salary_avg_5",
        "prev_salary_scaled",
        "manager_id_min_salary_scaled",
        F.when(F.col("salary_scaled") >= 62, "High").when(F.col("salary_scaled") >= 11, "Medium").otherwise("Low").alias("salary_scaled_level"),
    )
)

valid = (
    base
    .filter(F.col("salary") > 95000)
    .select(
        "*",
        (F.col("manager_id_min_salary_scaled") + F.col("prev_salary_scaled")).alias("manager_id_min_salary_scaled_plus_prev_salary_scaled"),
    )
)

scored = (
    valid
    .filter(F.col("salary_avg_5") != 89226)
    .select(
        "id",
        "salary_avg_5",
        "prev_salary_scaled",
        "manager_id_min_salary_scaled",
        F.row_number().over(w5).alias("event_seq"),
        F.cume_dist().over(w6).alias("salary_avg_5_cume_dist"),
        F.row_number().over(w7).alias("row_seq"),
    )
)

result = (
    scored
    .select(
        "id",
        "salary_avg_5",
        "salary_avg_5_cume_dist",
        F.sum("event_seq").over(w8).alias("running_sum_event_seq"),
        F.max("event_seq").over(w8).alias("running_max_event_seq"),
        F.count("event_seq").over(w8).alias("running_count_event_seq"),
        F.dense_rank().over(w9).alias("manager_id_min_salary_scaled_dense_rank"),
    )
)
