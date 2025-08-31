# How to Run Tests Locally
pytest tests/

# How to Run Tests in CI/CD

Add to your GitHub Actions / Azure DevOps pipeline:

GitHub Actions snippet
- name: Run Pytest
  run: pytest tests/ --maxfail=1 --disable-warnings -q

Azure DevOps snippet
- script: pytest tests/ --maxfail=1 --disable-warnings -q
  displayName: "Run Pytest"

## Benefits

✔️ Quality gates in CI/CD (deploy fails if tests fail)
✔️ Validates ingestion created raw tables, harmonize cleaned them, refined tables populated
✔️ Extensible → you can add more tests (schema drift, null checks, value ranges, etc.)
