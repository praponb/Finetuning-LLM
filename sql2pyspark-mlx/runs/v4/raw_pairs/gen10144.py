from pyspark.sql import functions as F

valid = (
    df_accounts
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-05-02"))) & F.col("balance").isNull())
    .withColumn("account_type_title", F.initcap(F.trim("account_type")))
    .withColumn("account_type_group", F.when(F.col("account_type").isin(["CREDIT", "CHECKING"]), "Tier 1").otherwise("Other"))
)

result = (
    valid
    .select("status", "id")
    .intersectAll(
        df_accounts
        .filter(F.col("customer_id").isNull())
        .select("status", "id")
    )
)
