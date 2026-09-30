from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "customer_country_2").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("country").orderBy("start_date", "id")
discount_by_customer_id = df_orders.groupBy("customer_id").agg(F.max("discount").alias("max_order_discount"))

enriched = (
    df_subscriptions
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .filter(F.col("d.age") <= 67)
    .select(
        "e.id",
        "e.customer_id",
        "e.plan",
        "e.start_date",
        "e.monthly_fee",
        F.col("d.email"),
        F.col("d.country"),
    )
)

with_dims = (
    enriched
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "inner")
    .select(
        "a.id",
        "a.customer_id",
        "a.plan",
        "a.start_date",
        "a.country",
        F.col("p.name"),
        F.col("p.country").alias("customer_country"),
        F.col("p.signup_date"),
    )
)

enriched_subscriptions = (
    with_dims
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .select(
        "src.id",
        "src.customer_id",
        "src.plan",
        "src.start_date",
        "src.country",
        "src.customer_country",
        F.col("c.country").alias("customer_country_2"),
    )
)

metrics = (
    enriched_subscriptions
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "country",
        "customer_country",
        "customer_country_2",
        F.last("customer_country", ignorenulls=True).over(w).alias("last_known_customer_country"),
        F.row_number().over(w_ordered).alias("event_seq"),
    )
)

result = (
    metrics
    .join(discount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "customer_country",
        "last_known_customer_country",
        "event_seq",
        F.when(F.col("plan").rlike("^(premium|standard)$"), "CORE").when(F.col("plan").rlike("^(basic|enterprise)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("plan_zone"),
        F.col("max_order_discount"),
    )
    .orderBy(F.col("max_order_discount").asc_nulls_first(), F.col("event_seq").desc())
)
