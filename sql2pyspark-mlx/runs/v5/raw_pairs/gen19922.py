from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "customer_id").orderBy(F.col("end_date").desc_nulls_first(), "id")
w2 = Window.partitionBy("customer_id", "status").orderBy(F.col("monthly_fee").desc_nulls_first(), "id")

latest = (
    df_subscriptions
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

joined = (
    latest
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id") == F.col("b.id"), "left")
    .filter(~F.col("b.city").isin(["London", "Bangkok"]))
    .select(
        "src.id",
        "src.customer_id",
        "src.plan",
        "src.end_date",
        "src.monthly_fee",
        "src.status",
        F.col("b.city"),
        F.col("b.email"),
        F.col("b.name"),
    )
)

joined_subscriptions = (
    joined
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id").eqNullSafe(F.col("b.id")), "left")
    .select("s.*", F.col("b.email").alias("customer_email"))
)

deduped = (
    joined_subscriptions
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        "monthly_fee",
        "status",
        "email",
        "name",
        "customer_email",
        F.when(F.col("name").rlike("^(Olivia Garcia|Emma Brown)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("name_zone"),
    )
)
