-- Run once in Databricks SQL or a notebook. Change `workspace` if needed.
CREATE SCHEMA IF NOT EXISTS workspace.datadoctor;
CREATE VOLUME IF NOT EXISTS workspace.datadoctor.source_files;
