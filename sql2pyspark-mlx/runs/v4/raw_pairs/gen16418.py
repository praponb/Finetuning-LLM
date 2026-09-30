from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())

with_last = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.salary"))
)

result = (
    with_last
    .select(
        "id",
        "location",
        "budget",
        "salary",
        F.when(F.col("location").rlike("^(Bangkok|Singapore)$"), "GROUP_B").when(F.col("location").rlike("^(Chiang Mai|New York)$"), "PRIORITY").otherwise("GROUP_A").alias("location_zone"),
    )
    .orderBy("location_zone")
)
