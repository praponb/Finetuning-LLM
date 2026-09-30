from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy(F.col("hire_date").desc_nulls_last(), "id")
w2 = Window.orderBy("salary", "id")
w3 = Window.orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("salary").desc(), "id")
w5 = Window.orderBy(F.col("cumulative_salary").asc_nulls_first())
w6 = Window.orderBy(F.col("cumulative_salary").desc_nulls_first())

base = (
    df_employees
    .filter(F.col("level").isNotNull() & F.col("dept_id").isin([1, 2, 31, 35]))
    .select(
        "id",
        "dept_id",
        "salary",
        "hire_date",
        "is_remote",
        F.upper("level").alias("level_upper"),
    )
)

deduped = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "salary", "level_upper")
)

metrics = (
    deduped
    .filter(F.col("salary") <= 117000)
    .select(
        "id",
        "level_upper",
        F.row_number().over(w2).alias("row_seq"),
        F.sum("salary").over(w3).alias("cumulative_salary"),
        F.row_number().over(w4).alias("event_seq"),
    )
)

matched_keys = (
    metrics
    .select("id")
    .intersect(
        df_employees
        .filter(F.col("is_remote"))
        .select("id")
    )
)

verified = (
    metrics
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter(F.col("cumulative_salary") < 329410)
    .select(
        "id",
        "row_seq",
        "cumulative_salary",
        (F.col("row_seq") - F.col("cumulative_salary")).alias("row_seq_minus_cumulative_salary"),
        (F.col("event_seq") - F.col("row_seq")).alias("event_seq_minus_row_seq"),
    )
)

cleaned = (
    filtered
    .filter(F.col("row_seq_minus_cumulative_salary").between(39, 70))
    .select(
        "*",
        F.when(F.col("row_seq_minus_cumulative_salary") >= 70, "High").when(F.col("row_seq_minus_cumulative_salary") >= 17, "Medium").otherwise("Low").alias("row_seq_minus_cumulative_salary_band"),
    )
)

metrics_employees = (
    cleaned
    .select(
        "id",
        "row_seq",
        "cumulative_salary",
        "event_seq_minus_row_seq",
        F.dense_rank().over(w5).alias("cumulative_salary_dense_rank"),
        F.rank().over(w6).alias("cumulative_salary_rank"),
    )
)

result = (
    metrics_employees
    .select("id", "row_seq", "cumulative_salary", "event_seq_minus_row_seq")
)
