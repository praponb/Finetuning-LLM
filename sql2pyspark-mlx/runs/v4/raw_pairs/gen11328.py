from pyspark.sql import functions as F

base = (
    df_employees
    .filter(F.col("hire_date") >= F.lit("2023-03-20").cast("date"))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "hire_date",
        "is_remote",
        F.split(F.lower("email"), "@").getItem(0).alias("email_user"),
    )
)

result = (
    base
    .withColumn("week_start", F.date_trunc("WEEK", "hire_date"))
    .groupingSets(
        [["manager_id", "week_start"], ["manager_id"]],
        "manager_id",
        "week_start",
    )
    .agg(
        F.countDistinct("manager_id").alias("distinct_manager_ids"),
        F.count("*").alias("event_count"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("distinct_manager_ids").desc())
)
