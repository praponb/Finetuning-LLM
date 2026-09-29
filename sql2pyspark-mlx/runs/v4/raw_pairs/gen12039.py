from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("budget").desc(), "id")
w3 = Window.partitionBy("location").orderBy("first_known_budget", "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())

scored = (
    df_departments
    .withColumn("first_known_budget", F.first("budget", ignorenulls=True).over(w_ordered))
    .withColumn("budget_bucket", F.ntile(5).over(w2))
)

latest = (
    scored
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

with_last = (
    latest
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.level"), F.col("c.salary"))
)

result = (
    with_last
    .groupBy("location")
    .pivot("dept_name", ["HR", "Engineering", "Legal"])
    .agg(F.max("budget_bucket").alias("value_sum"))
)
