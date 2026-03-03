DDL = """
create table if not exists mosstat_cpi (
    year int not null,
    month text not null,
    cpi_index_prev_month numeric,
    primary key (year, month)
);

create table if not exists mosstat_income (
    indicator text not null,
    year int not null,
    value numeric,
    primary key (indicator, year)
);

create table if not exists mosstat_poverty (
    year int primary key,
    poverty_share_percent numeric
);

create table if not exists mosstat_morbidity (
    disease_class text not null,
    year int not null,
    cases_total numeric,
    primary key (disease_class, year)
);

create table if not exists mosstat_medstaff (
    year int primary key,
    doctors_total numeric,
    doctors_per_10k numeric,
    nurses_total numeric,
    nurses_per_10k numeric
);
"""
