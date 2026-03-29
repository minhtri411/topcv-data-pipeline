CREATE DATABASE airflow;
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS raw.topcv_jobs (
    raw_id SERIAL PRIMARY KEY,
    
    -- Basic Job Info
    title TEXT,
    job_url TEXT,
    
    -- Basic Company Info 
    company_url TEXT,
    
    -- Job Details
    salary TEXT,
    location TEXT,
    experience TEXT,
    deadline TEXT,
    tags TEXT,
    
    -- Job Descriptions
    desc_mota TEXT,
    desc_yeucau TEXT,
    desc_quyenloi TEXT,
    
    -- Working Context
    working_addresses TEXT,
    working_times TEXT,
    
    -- Company Deep Dive 
    company_name_full TEXT,
    company_website TEXT,
    company_size TEXT,
    company_followers TEXT,
    company_industry TEXT,
    company_address TEXT,
    company_description TEXT,
    
    -- Metadata 
    crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);