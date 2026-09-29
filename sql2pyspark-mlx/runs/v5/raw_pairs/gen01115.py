from pyspark.sql import functions as F

filtered = (
    df_customers
    .filter(F.col("credit_limit") > 12300)
    .withColumn("age_per_credit_limit", F.round((F.col("age") / F.col("credit_limit")), 2))
    .withColumn("is_active_flag", F.col("is_active").cast("int"))
)

result = (
    filtered
    .select("id", "segment", "age_per_credit_limit", "credit_limit")
    .unpivot(
        ids=["id", "segment"],
        values=["age_per_credit_limit", "credit_limit"],
        variableColumnName="kpi",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
