# Testing

## Postgres installation

	sudo apt install postgresql-12 postgresql-12-postgis-3
	sudo pg_ctlcluster 12 main start

## Creating users

Switch user to `postgres`:

	sudo su postgres -

Create user `erdetest` and a database.

	psql postgres -c "create role erdetest with login password 'erdetest'; alter role erdetest createdb;"
	createdb erdetest -O erdetest && psql erdetest -c "create extension postgis;"
