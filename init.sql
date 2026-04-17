CREATE DATABASE airflow;
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS raw.topcv_jobs (
    raw_id SERIAL PRIMARY KEY,

    title TEXT,
    job_url TEXT,

    company_url TEXT,

    salary TEXT,
    location TEXT,
    experience TEXT,
    deadline TEXT,
    tags TEXT,

    desc_mota TEXT,
    desc_yeucau TEXT,
    desc_quyenloi TEXT,

    working_addresses TEXT,
    working_times TEXT,

    company_name_full TEXT,
    company_website TEXT,
    company_size TEXT,
    company_followers TEXT,
    company_industry TEXT,
    company_address TEXT,
    company_description TEXT,

    crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);