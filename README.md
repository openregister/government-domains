# government-domains

A repository for government domains data

# requirements

- Java 1.8+
- Postgres DB 9.4

# Recreate data file

- create postgres database 'domains' if not exist already `createdb domains`
- Run script `./create-data-file.sh`
- output `domains.txt` is created at `data/domains/domains.txt`
