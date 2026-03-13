# Great Expectations Validation Suite

Validates data quality for the Loan Application Data Warehouse.

## Structure
```
great_expectations_suite/
├── run_validations.py        ← main entry point
├── utils/
│   └── connection.py         ← Postgres connection setup
└── expectations/
    ├── dim_customer.py       ← expectations for dim_customer table
    ├── fact_loans.py         ← expectations for fact_loans table
    └── dim_bureau.py         ← expectations for dim_bureau table
```

## How to run
```bash
python great_expectations_suite/run_validations.py
```

## What gets validated

| Table | Checks |
|---|---|
| dim_customer | not_null, unique on customer_key, gender values, income/credit > 0, target is 0/1, row count 300k-320k |
| fact_loans | not_null on customer_key, all metrics > 0, row count 1.4M-1.5M |
| dim_bureau | not_null, unique on bureau_id, row count 1.7M-1.75M |

## Data Docs
After running, open the HTML report at:
`gx/uncommitted/data_docs/local_site/index.html`
