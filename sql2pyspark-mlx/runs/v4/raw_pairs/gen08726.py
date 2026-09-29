from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("level")
    .pivot("job_title", ["Manager", "Designer"])
    .agg(F.avg("salary"))
    .select(
        "level",
        F.col("Manager").alias("manager"),
        F.col("Designer").alias("designer"),
    )
)
