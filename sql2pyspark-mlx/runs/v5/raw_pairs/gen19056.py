from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("salary").desc())
w2 = Window.partitionBy("manager_id")
w3 = Window.partitionBy("job_title", "level").orderBy(F.col("salary").desc(), "id")
w4 = Window.partitionBy("level").orderBy("salary")
w5 = Window.partitionBy("job_title").orderBy(F.col("salary_rank").desc(), "id")
w6 = Window.partitionBy("job_title").orderBy(F.col("salary_share").desc_nulls_first(), "id")
w7 = Window.partitionBy("job_title").orderBy(F.col("salary_share").desc_nulls_first())
w8 = Window.partitionBy("job_title").orderBy(F.col("salary").desc(), "id")

cleaned = (
    df_employees
    .filter(F.col("manager_id").isNotNull() & (F.col("level") == "L2"))
    .select(
        "id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.concat_ws("-", "email", "level").alias("email_level_key"),
    )
)

with_windows = (
    cleaned
    .select(
        "id",
        "job_title",
        "salary",
        "is_remote",
        "email_level_key",
        F.dense_rank().over(w).alias("salary_dense_rank"),
        F.round((F.col("salary") / F.sum("salary").over(w2)), 4).alias("salary_share"),
        F.row_number().over(w3).alias("row_seq"),
        F.rank().over(w4).alias("salary_rank"),
    )
)

deduped = (
    with_windows
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
)

deduped_employees = (
    deduped
    .withColumn("row_num_2", F.row_number().over(w6))
    .filter(F.col("row_num_2") == 1)
)

ranked = (
    deduped_employees
    .withColumn("rn", F.rank().over(w7))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

deduped_employees_2 = (
    ranked
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

grouped = (
    deduped_employees_2
    .withColumn("salary_rank_tier", F.when(F.col("salary_rank") >= 8, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["job_title", "salary_rank_tier"], ["job_title"], []],
        "job_title",
        "salary_rank_tier",
    )
    .agg(
        F.stddev_samp("salary").alias("salary_stddev"),
        F.avg(F.when(F.col("job_title") == "Intern", F.col("salary_share"))).alias("intern_avg_salary_share"),
        F.round(F.sum("salary_share") / F.nullif(F.count(F.when(F.col("job_title") == "Designer", 1)), F.lit(0)), 2).alias("salary_share_per_designer"),
        F.count("*").alias("cnt"),
        F.grouping_id().alias("gid"),
    )
)

result = (
    grouped
    .select(
        "job_title",
        "salary_stddev",
        "intern_avg_salary_share",
        "cnt",
        "gid",
        F.when(F.col("job_title").rlike("^(Intern|Director)$"), "EXTENDED").otherwise("GROUP_B").alias("job_title_zone"),
    )
    .orderBy(F.col("intern_avg_salary_share").desc_nulls_last())
)
