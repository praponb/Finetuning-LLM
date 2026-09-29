from pyspark.sql import functions as F

valid = (
    df_customers
    .filter((F.col("signup_date") >= F.to_date(F.lit("2023-03-29"))) & (~F.col("segment").eqNullSafe(F.lit("Corporate"))))
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        F.round((F.col("credit_limit") / F.col("age")), 2).alias("credit_limit_per_age"),
    )
)

result = (
    valid
    .select("id", "segment", "credit_limit_per_age", "credit_limit")
    .unpivot(
        ids=["id", "segment"],
        values=["credit_limit_per_age", "credit_limit"],
        variableColumnName="metric",
        valueColumnName="value",
    )
)
