from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        F.concat_ws("/", "location", "dept_name").alias("location_dept_name_key"),
        F.when(F.col("budget") >= 4290000, "High").when(F.col("budget") >= 682000, "Medium").otherwise("Low").alias("budget_tier"),
    )
)
