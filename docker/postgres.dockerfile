FROM postgres:14

LABEL author="Vladislav Zybkin "
LABEL description="Postgres Image for deduplication storage"
LABEL version="1.0"

COPY *.sql /docker-entrypoint-initdb.d/