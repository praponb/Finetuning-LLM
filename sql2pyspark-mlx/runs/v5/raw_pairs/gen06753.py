from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("salary", "id")
w2 = Window.partitionBy("location", "level")
w3 = Window.orderBy("prev_salary", "id")
w4 = Window.partitionBy("level").orderBy("prev_salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("level").orderBy("event_seq", "id")
w6 = Window.partitionBy("dept_id").orderBy("event_seq", "id")
w7 = Window.partitionBy("location", "dept_name").orderBy(F.col("event_seq_bucket").desc(), "id")
w8 = Window.orderBy(F.col("next_salary").desc_nulls_last())
w9 = Window.partitionBy("dept_name").orderBy(F.col("next_event_seq_bucket").desc(), "id")
w10 = Window.orderBy(F.col("prev_event_seq_bucket").desc(), "id")
w11 = Window.partitionBy("level").orderBy(F.col("next_salary").desc(), "id")

joined = (
    df_employees
    .alias("s")
    .join(df_departments.alias("p"), F.col("s.dept_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.location"), F.col("p.dept_name"))
)

latest = (
    joined
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

scored = (
    latest
    .select(
        "*",
        F.lag("salary").over(w).alias("prev_salary"),
        F.row_number().over(w).alias("event_seq"),
        F.round((F.col("salary") / F.sum("salary").over(w2)), 4).alias("salary_share"),
    )
)

scored_employees = (
    scored
    .filter(F.col("hire_date") <= "2024-01-18")
    .select(
        "*",
        F.lag("event_seq", 2, 0).over(w3).alias("prev_event_seq"),
        F.last("level", ignorenulls=True).over(w4).alias("last_known_level"),
        F.ntile(3).over(w5).alias("event_seq_bucket"),
    )
)

metrics = (
    scored_employees
    .select(
        "id",
        "email",
        "level",
        "location",
        "dept_name",
        "prev_salary",
        "event_seq",
        "last_known_level",
        "event_seq_bucket",
        F.lead("salary").over(w5).alias("next_salary"),
        F.lag("event_seq_bucket").over(w5).alias("prev_event_seq_bucket"),
        F.row_number().over(w5).alias("seq_no"),
        F.lead("event_seq_bucket", 1, 0).over(w6).alias("next_event_seq_bucket"),
    )
)

enriched = (
    metrics
    .withColumn("row_seq", F.row_number().over(w7))
    .withColumn("next_salary_rank", F.rank().over(w8))
    .withColumn("event_seq_2", F.row_number().over(w9))
    .withColumn("event_seq_3", F.row_number().over(w10))
    .withColumn("seq_no_2", F.row_number().over(w11))
)

remaining_keys = (
    enriched
    .select("level", "id")
    .intersectAll(
        df_employees
        .filter(F.col("level") == "L1")
        .select("level", "id")
    )
)

verified = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.level") == F.col("k.level")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy("location", "level", "dept_name")
    .agg(
        F.count(F.when(F.col("dept_name") == "Sales", 1)).alias("sales_count"),
        F.sum(F.when(F.col("location") == "London", F.col("row_seq")).otherwise(0)).alias("london_row_seq"),
    )
    .filter(F.col("sales_count") > 1)
    .orderBy(F.col("london_row_seq").desc())
)
