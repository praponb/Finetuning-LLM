from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("level") != "L3") & F.col("salary").between(56000, 87000))
    .select(
        "id",
        "manager_id",
        "email",
        F.date_format("hire_date", "yyyy-MM").alias("hire_date_label"),
        F.date_add("hire_date", 39).alias("hire_date_plus_39d"),
    )
)
