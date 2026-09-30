from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy(F.col("salary").desc())
w2 = Window.partitionBy("manager_id", "level").orderBy("salary_rounded", "id")

filtered = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2023-10-05").cast("date")) & F.col("level").isNotNull() & (F.col("level") == "L1"))
    .select(
        "*",
        F.round("salary", 1).alias("salary_rounded"),
        F.coalesce("level", F.lit("N/A")).alias("level_clean"),
    )
)

result = (
    filtered
    .withColumn("salary_dense_rank", F.dense_rank().over(w_running))
    .withColumn("next_salary", F.lead("salary", 2, 0).over(w2))
)
