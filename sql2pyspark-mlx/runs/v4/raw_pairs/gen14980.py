from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("dept_name").orderBy(F.col("employee_salary").desc_nulls_first(), "id")
w3 = Window.partitionBy("dept_name", "location").orderBy(F.col("employee_salary").desc(), "id")

base = (
    df_departments
    .filter(F.col("location").isNotNull())
    .withColumn("dept_name_location", F.concat("dept_name", F.lit(", "), "location"))
)

with_last = (
    base
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.hire_date"), F.col("c.salary").alias("employee_salary"))
)

deduped = (
    with_last
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

deduped_departments = (
    deduped
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    deduped_departments
    .groupBy("dept_name")
    .pivot("location", ["Chiang Mai", "Tokyo", "New York"])
    .agg(F.max("employee_salary").alias("total"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "dept_name",
        F.col("Chiang Mai_total").alias("chiang_mai_total"),
        F.col("Chiang Mai_cnt").alias("chiang_mai_cnt"),
        F.col("Tokyo_total").alias("tokyo_total"),
        F.col("Tokyo_cnt").alias("tokyo_cnt"),
        F.col("New York_total").alias("new_york_total"),
        F.col("New York_cnt").alias("new_york_cnt"),
    )
)
