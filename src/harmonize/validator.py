from pyspark.sql import functions as F

def apply_validations(df, validations):
    """
    Apply validation rules to DataFrame
    """
    results = []
    for v in validations:
        col, rule, action = v["column"], v["rule"], v.get("action", "warn")

        invalid_count = df.filter(F.expr(f"NOT ({rule})")).count()
        if invalid_count > 0:
            if action == "fail":
                raise Exception(f"❌ Validation failed: {rule} ({invalid_count} invalid rows)")
            elif action == "drop":
                df = df.filter(F.expr(rule))
                print(f"⚠️ Dropped {invalid_count} invalid rows for rule: {rule}")
            else:
                print(f"⚠️ Warning: {invalid_count} rows failed validation: {rule}")

        results.append({"rule": rule, "invalid_count": invalid_count, "action": action})
    return df, results
