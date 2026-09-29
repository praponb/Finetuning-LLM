from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "manager_id").orderBy(F.col("is_analyst").asc_nulls_first(), "id")

filtered = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-04-18").cast("date")) & F.col("manager_id").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "hire_date",
        "is_remote",
        "level",
        F.when(F.col("job_title").isin(["Analyst", "Manager"]), "Group A").otherwise("Other").alias("job_title_group"),
        F.when(F.col("job_title") == "Analyst", 1).otherwise(0).alias("is_analyst"),
        F.coalesce("level", F.lit("NONE")).alias("level_filled"),
    )
)

result = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)
