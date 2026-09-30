from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "status").orderBy(F.col("monthly_fee").desc(), "id")

cleaned = (
    df_subscriptions
    .filter((F.col("end_date") > F.lit("2023-08-17").cast("date")) & F.col("customer_id").isNotNull())
    .withColumn("clean_status", F.trim("status"))
)

enriched = (
    cleaned
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .select(
        "src.id",
        "src.customer_id",
        "src.plan",
        "src.monthly_fee",
        "src.status",
        "src.clean_status",
        F.col("c.name"),
        F.col("c.age"),
        F.col("c.city"),
    )
)

latest = (
    enriched
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

kept_keys = (
    latest
    .select("status", "customer_id")
    .subtract(
        df_subscriptions
        .filter(F.col("status").isin(["CANCELLED", "PAUSED", "EXPIRED"]))
        .select("status", "customer_id")
    )
)

reconciled = (
    latest
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "id",
        "monthly_fee",
        "clean_status",
        "age",
        "city",
        "row_num",
        F.when(F.col("city").rlike("^(London|Singapore)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("city_zone"),
    )
    .orderBy(F.col("city").asc_nulls_first())
)
