AGMARK/
│
├── .env                          ← secrets (gitignored)
├── .gitignore
├── requirements.txt
├── README.md
│
├── src/
│   ├── bronze/
│   │   ├── ingest.py             ← API fetch + upsert (your current file, cleaned up)
│   │   └── schema.py             ← SQLAlchemy table definitions + create_tables()
│   │
│   ├── silver/
│   │   └── transform.sql         ← Star schema DDL + views/CTEs
│   │
│   ├── analytics/
│   │   ├── cointegration.py      ← Module A: Engle-Granger tests
│   │   └── anomaly_detection.py  ← Module B: Isolation Forest
│   │
│   └── utils/
│       ├── database.py                 ← get_engine(), session factory
│       ├── logger.py             ← logging config (the part you asked about)
│       └── watermark.py          ← get_watermark(), set_watermark()
│
├── sql/
│   ├── star_schema.sql           ← Dim/Fact table DDL
│   └── sample_queries.sql        ← ad-hoc validation queries
│
├── notebooks/
│   └── eda.ipynb                 ← exploratory analysis only, never production logic
│
└── tests/
    └── test_ingest.py            ← unit tests (even 2-3 basic ones impress interviewers)
