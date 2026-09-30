from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter(~F.col("a.account_type").isin(["CREDIT", "CHECKING", "LOAN"]) | F.col("c.name").like("%Sri%"))
    .select(
        F.col("a.id"),
        F.col("a.status"),
        F.col("a.account_type"),
        F.col("a.opened_date"),
        F.col("c.age"),
        F.initcap(F.trim(F.col("a.account_type"))).alias("account_type_title"),
    )
    .orderBy("age")
)
