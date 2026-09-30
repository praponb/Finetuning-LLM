from pyspark.sql import functions as F

valid = (
    df_departments
    .filter(F.col("location").isNotNull())
    .withColumn("dept_name_length", F.length("dept_name"))
    .withColumn("dept_name_group", F.when(F.col("dept_name").isin(["Marketing", "Legal"]), "Group A").otherwise("Other"))
)

result = (
    valid
    .select("id", "location")
    .subtract(
        df_departments
        .filter(F.col("dept_name") == "Sales")
        .select("id", "location")
    )
)
