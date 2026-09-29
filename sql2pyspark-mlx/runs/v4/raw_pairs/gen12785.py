from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "customer_id").orderBy(F.col("monthly_fee").desc_nulls_first(), "id")
w2 = Window.partitionBy("status").orderBy(F.col("end_date").asc_nulls_last(), "id")

ranked = (
    df_subscriptions
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

enriched = (
    ranked
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .select("a.*", F.col("c.name"))
)

latest = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        "monthly_fee",
        "status",
        "name",
        "latest_rank",
        F.when(F.col("name").rlike("^(Noah Park|FRANK MILLER)$"), "GROUP_A").when(F.col("name").rlike("^(Alice Wong|bob smith)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("name_zone"),
    )
)
