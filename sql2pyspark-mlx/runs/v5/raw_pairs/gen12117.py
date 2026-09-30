from pyspark.sql import functions as F

base = (
    df_departments
    .filter(F.col("location").isNotNull() & (F.col("budget") > 1868000))
    .select("*", F.coalesce("budget", F.lit(0)).alias("budget_filled"))
)

result = (
    base
    .select(
        "id",
        "location",
        F.col("budget_filled").alias("BUDGET_FILLED_VAL"),
        F.col("budget").alias("BUDGET_VAL"),
    )
    .unpivot(
        ids=["id", "location"],
        values=["BUDGET_FILLED_VAL", "BUDGET_VAL"],
        variableColumnName="measure",
        valueColumnName="value",
    )
)
