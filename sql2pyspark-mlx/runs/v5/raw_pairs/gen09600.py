from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "customer_id").orderBy(F.col("signup_date").desc(), "id")

cleaned = (
    df_subscriptions
    .filter((F.col("end_date") > F.to_date(F.lit("2023-11-07"))) & F.col("monthly_fee").between(150, 190))
    .withColumn("monthly_fee_int", F.col("monthly_fee").cast("int"))
)

with_dims = (
    cleaned
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .select("s.*", F.col("c.credit_limit"), F.col("c.signup_date"), F.col("c.email"))
)

ranked = (
    with_dims
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

base = (
    ranked
    .filter((F.col("end_date") >= F.to_date(F.lit("2023-07-09"))) & F.col("customer_id").isNotNull())
    .withColumn("plan_title", F.initcap(F.trim("plan")))
    .withColumn("signup_date_label", F.date_format("signup_date", "MMM yyyy"))
)

result = (
    base
    .select("id", "plan", "credit_limit", F.col("monthly_fee").alias("MONTHLY_FEE"))
    .unpivot(
        ids=["id", "plan"],
        values=["credit_limit", "MONTHLY_FEE"],
        variableColumnName="metric_name",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
