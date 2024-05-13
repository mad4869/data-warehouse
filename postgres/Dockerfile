FROM postgres:latest

COPY ./init.sql /docker-entrypoint-initdb.d/

ENV POSTGRES_USER=mad4869
ENV POSTGRES_PASSWORD=password123
ENV POSTGRES_DB=dwh