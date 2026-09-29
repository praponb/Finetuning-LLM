from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())

filtered = (
    df_departments
    .filter(F.col("location").isNotNull())
    .select(
        "*",
        F.initcap(F.trim("location")).alias("location_title"),
        F.when(F.col("dept_name") == "Engineering", 1).otherwise(0).alias("is_engineering"),
        F.coalesce("dept_name", F.lit("NONE")).alias("dept_name_clean"),
    )
)

result = (
    filtered
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.salary"), F.col("c.level"))
)
