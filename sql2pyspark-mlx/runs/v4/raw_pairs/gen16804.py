from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("dept_id")
    .pivot("job_title", ["Designer", "Engineer"])
    .agg(F.max("salary").alias("amount"), F.count(F.lit(1)).alias("n"))
    .select(
        "dept_id",
        F.col("Designer_amount").alias("designer_amount"),
        F.col("Designer_n").alias("designer_n"),
        F.col("Engineer_amount").alias("engineer_amount"),
        F.col("Engineer_n").alias("engineer_n"),
    )
)
