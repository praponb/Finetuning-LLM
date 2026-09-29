from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("dept_name").isin(["Marketing", "Legal", "Finance"]) & (F.col("dept_name").isNotNull() | F.col("location").isin(["Bangkok", "London"])))
    .select(
        "id",
        "dept_name",
        F.when(F.col("dept_name") == "Sales", 1).otherwise(0).alias("is_sales"),
    )
    .orderBy("is_sales")
)
