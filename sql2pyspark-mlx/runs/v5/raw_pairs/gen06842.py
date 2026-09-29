from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("level", "manager_id").orderBy("salary", "id")
w2 = Window.partitionBy("level").orderBy("row_seq", "id")
w3 = Window.orderBy("hire_date", "id")
w4 = Window.orderBy(F.col("prev_salary").desc())
w5 = Window.partitionBy("level").orderBy("prev_salary", "id")
w6 = Window.orderBy("prev_salary", "id")

valid = (
    df_employees
    .filter(F.col("salary").between(130000, 148000))
    .withColumn("email_title", F.initcap(F.trim("email")))
    .withColumn("email_upper", F.upper("email"))
)

scored = (
    valid
    .select(
        "id",
        "full_name",
        "hire_date",
        "is_remote",
        "level",
        "email_title",
        F.lag("salary").over(w_recent).alias("prev_salary"),
        F.row_number().over(w_recent).alias("row_seq"),
    )
)

ranked = (
    scored
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

metrics = (
    ranked
    .withColumn("event_seq", F.row_number().over(w3))
    .withColumn("prev_salary_rank", F.rank().over(w4))
    .withColumn("next_row_seq", F.lead("row_seq", 1, 0).over(w5))
    .withColumn("prev_salary_bucket", F.ntile(10).over(w6))
    .withColumn("seq_no", F.row_number().over(w6))
)

valid_employees = (
    metrics
    .filter(F.col("hire_date") >= F.lit("2023-08-13").cast("date"))
    .select(
        "id",
        "full_name",
        "is_remote",
        "level",
        "email_title",
        "prev_salary",
        "event_seq",
        "prev_salary_rank",
        "next_row_seq",
        "prev_salary_bucket",
        F.coalesce("next_row_seq", F.lit(0)).alias("next_row_seq_filled"),
        F.coalesce("level", F.lit("N/A")).alias("resolved_level"),
    )
)

result = (
    valid_employees
    .select(
        "id",
        "full_name",
        "is_remote",
        "prev_salary",
        "prev_salary_rank",
        "next_row_seq",
        "prev_salary_bucket",
        F.when(F.col("full_name").rlike("^(Mia Rossi|Kanya Srisuk)$"), "PRIORITY").when(F.col("full_name").rlike("^(Alice Wong|bob smith)$"), "GROUP_A").otherwise("GROUP_B").alias("full_name_zone"),
    )
    .orderBy(F.col("id").desc_nulls_first())
)
