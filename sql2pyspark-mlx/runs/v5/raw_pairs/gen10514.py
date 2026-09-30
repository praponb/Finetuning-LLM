from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("status_length").desc_nulls_last(), "id")

base = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.length("status").alias("status_length"),
    )
)

base_subscriptions = (
    base
    .filter((F.col("end_date") >= F.to_date(F.lit("2023-05-08"))) & (F.col("end_date") > "2023-11-01"))
    .withColumn("status_plan", F.concat("status", F.lit(" "), "plan"))
)

joined = (
    base_subscriptions
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .filter(F.col("p.segment").isin(["Corporate", "SMB", "Consumer"]))
    .select("src.*", F.col("p.credit_limit"), F.col("p.signup_date"))
)

deduped = (
    joined
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    deduped
    .select(
        "id",
        "customer_id",
        F.col("credit_limit").alias("CREDIT_LIMIT_VAL"),
        "monthly_fee",
    )
    .unpivot(
        ids=["id", "customer_id"],
        values=["CREDIT_LIMIT_VAL", "monthly_fee"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
