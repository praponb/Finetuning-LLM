from pyspark.sql import functions as F

valid = (
    df_employees
    .filter(F.col("job_title") != "Manager")
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "job_title",
        "salary",
        "level",
        F.when(F.col("job_title").isin(["Director", "Manager"]), "Core").otherwise("Other").alias("job_title_group"),
    )
)

result = (
    valid
    .groupBy("manager_id")
    .pivot("job_title", ["Manager", "Engineer"])
    .agg(F.sum("salary"))
)
