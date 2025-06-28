# qPCRInsight-Backend

.
├── backend/
│   ├── __init__.py
│   ├── core/
│   │   └── config.py
│   ├── ingest/
│   │   ├── __init__.py
│   │   └── parser.py
│   ├── qc/
│   │   └── replicate.py
│   ├── analysis/
│   │   ├── __init__.py
│   │   ├── normalize.py
│   │   └── fold_change.py
│   ├── stats/
│   │   ├── tests.py
│   │   └── posthoc.py
│   ├── plots/
│   │   └── factory.py
│   ├── report/
│   │   ├── utils.py
│   │   ├── builder.py
│   │   └── packager.py
│   ├── services/
│   │   ├── storage.py
│   │   └── logging.py
│   ├── auth/
│   │   └── jwt.py
│   └── api/
│       ├── __init__.py
│       └── routes/
│           ├── files.py
│           └── jobs.py
├── lambdas/
│   ├── upload_presign/handler.py
│   ├── ingest_queue/handler.py
│   ├── analysis_runner/handler.py
│   ├── stats_worker/handler.py
│   ├── plot_worker/handler.py
│   ├── report_builder/handler.py
│   └── notify_complete/handler.py
├── infra/
│   ├── template.yaml
│   └── stepfn_analysis.asl.json
├── .github/
│   └── workflows/ci.yml
└── tests/
   └── test_parse_xlsx.py