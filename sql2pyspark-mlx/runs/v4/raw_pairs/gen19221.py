from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("hire_date").desc_nulls_last(), "id")
w2 = Window.partitionBy("job_title").orderBy(F.col("salary").desc(), "id")

with_dims = (
    df_employees
    .alias("src")
    .join(df_employees.alias("d"), F.col("src.manager_id") == F.col("d.id"), "inner")
    .select(
        "src.*",
        F.col("d.email").alias("employee_email"),
        F.col("d.level").alias("employee_level"),
    )
)

ranked = (
    with_dims
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

cleaned = (
    ranked
    .filter(F.col("job_title").isNotNull())
    .withColumn("employee_email_lower", F.lower("employee_email"))
    .withColumn("email_lower", F.lower("email"))
)

deduped = (
    cleaned
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .withColumn("salary_tier", F.when(F.col("salary") >= 133000, "HIGH").otherwise("LOW"))
    .rollup("employee_level", "salary_tier")
    .agg(
        F.count(F.when(F.col("employee_level") == "L3", 1)).alias("l3_rows"),
        F.count("*").alias("cnt"),
        F.grouping("employee_level").alias("grouping_id"),
    )
    .filter(F.col("cnt") < 1)
)
