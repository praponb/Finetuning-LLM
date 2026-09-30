from pyspark.sql import functions as F

excluded_customers = df_customers.filter(F.col("segment") == "Corporate").select("id")

result = (
    df_accounts.alias("a")
    .join(excluded_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left_anti")
    .filter(F.col("a.customer_id").isNotNull() | F.lit(excluded_customers.isEmpty()))
    .filter(F.col("a.customer_id") == 35)
    .select(F.col("a.id"), F.col("a.status"), F.col("a.opened_date"))
    .orderBy("opened_date", "status", "id")
    .limit(20)
)
