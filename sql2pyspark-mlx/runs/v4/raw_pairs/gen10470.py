from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("dept_id", "level")
    .pivot("job_title", ["Manager", "Engineer", "Intern"])
    .agg(F.max("salary"))
    .select(
        "dept_id",
        "level",
        F.col("Manager").alias("manager"),
        F.col("Engineer").alias("engineer"),
        F.col("Intern").alias("intern"),
    )
)
