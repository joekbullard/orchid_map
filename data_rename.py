
import psycopg2
from config import config
from psycopg2 import sql


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


    pg_conn = connect()

    with pg_conn:
        with pg_conn.cursor() as cur:
            
            cur.execute(f"Select * FROM species_list.orchid_list LIMIT 0")
            col_names = [desc[0] for desc in cur.description]

            for i in col_names:
                for
                
                if ' ' in i:
                    try:
                        query = sql.SQL("ALTER TABLE species_list.orchid_list RENAME {col1} to {col2}").format(
                            col1=sql.Identifier(i),
                            col2=sql.Identifier(i.replace(' ', '_').replace('/','_').replace('('))
                        )
                        cur.execute(query)
                    except Exception as e:
                        print(e)
                        pass
                    finally:
                        pg_conn.commit()
                else:
                    pass


if __name__ == "__main__":
    main()