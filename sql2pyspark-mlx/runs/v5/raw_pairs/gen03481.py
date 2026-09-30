from pyspark.sql import functions as F

excluded_customers = df_customers.filter(F.upper(F.trim("name")) == "IVY CHEN").select("id")

result = (
    df_accounts.alias("a")
    .join(excluded_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left_anti")
    .filter(F.col("a.customer_id").isNotNull() | F.lit(excluded_customers.isEmpty()))
    .filter(F.col("a.account_type").isin(["CHECKING", "CREDIT", "SAVINGS"]))
    .select(F.col("a.id"), F.col("a.account_type"), F.col("a.customer_id"))
)
