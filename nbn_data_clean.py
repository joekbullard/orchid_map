#!/usr/bin/env python
"""
Script to clean data downloaded from NBN and produce output shapefile
"""

import argparse
import csv
import psycopg2
from config import config
from psycopg2.extensions import AsIs


def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
		
        # create a cursor
        return conn

    except (Exception, psycopg2.DatabaseError) as e:
        print(e)   



def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("csv_file", help="Add file path of csv")
    parser.add_argument("table_name", help="Enter table name")
    args = parser.parse_args()

    csv_file = args.csv_file
    table_name = args.table_name

    pg_conn = connect()

    create_table = f"""
                DROP TABLE species_list.{table_name};
                CREATE TABLE species_list.{table_name}(
                nbn_atlas_record_id TEXT PRIMARY KEY,
                occurrence_id TEXT,
                licence TEXT,
                rightsholder TEXT,
                scientific_name TEXT,
                taxon_authot TEXT,
                name_qualifier TEXT,
                common_name TEXT,
                species_id TEXT,
                taxon_rank TEXT,
                occurrence_status TEXT,
                start_date DATE,
                start_date_day INT,
                start_date_month INT,
                start_date_year INT,
                end_date DATE,
                end_date_day INT,
                end_date_month INT,
                end_date_year INT,
                locality TEXT,
                osgr TEXT,
                lat FLOAT,
                lon FLOAT,
                spatial_accuracy INT,
                depth INT,
                recorder TEXT,
                determiner TEXT,
                invidual_count TEXT,
                abundance TEXT,
                abundance_scale TEXT,
                organism_scope TEXT,
                organism_remarks TEXT,
                sex TEXT,
                life_stage TEXT,
                occurrence_remarks TEXT,
                id_verification_status TEXT,
                record_basis TEXT,
                survey_key TEXT,
                dataset_name TEXT,
                dataset_id TEXT,
                data_provider TEXT,
                data_provider_id TEXT,
                institution_code TEXT,
                kingdom TEXT,
                taxon_phylum TEXT,
                taxon_class TEXT,
                taxon_order TEXT,
                family TEXT,
                genus TEXT,
                centisquare TEXT,
                hectad TEXT,
                tetrad TEXT,
                monad TEXT,
                country TEXT,
                county TEXT,
                vitality TEXT
                );"""

    insert_query = f'INSERT INTO species_list.{table_name}(%s)vVALUES(%s)'


    with pg_conn:
        with pg_conn.cursor() as cur:

            cur.execute(create_table)
            pg_conn.commit()
            
            cur.execute(f"Select * FROM species_list.{table_name} LIMIT 0")
            col_names = [desc[0] for desc in cur.description]

            with open(csv_file, "r") as source:
                reader = csv.reader(source)
                next(reader)

                for r in reader:
                    zip(col_names)
                    try:
                        print(cur.execute(
                            f'INSERT INTO species_list.{table_name}(%s) VALUES(%s)', (r,)))
                    except Exception as e:
                        print(e)
                        pass
                    finally:
                        pg_conn.commit()


if __name__ == "__main__":
    main()