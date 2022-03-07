import psycopg2
import LinPy
import click
import re
import os

from typing import List

from operator import itemgetter


def ask_yes_no(question: str) -> bool:
    ans = None
    while ans not in ("y", "yes", "n", "no", "Y", "YES", "N", "NO"):
        ans = input(f"{question} ? [y/n] ").lower()
        if ans in ("y", "yes"):
            return True
        elif ans in ("n", "no"):
            return False

def ask_user_to_select(items_name: str, options: List[str]) -> List[str]:
    print(f"Choose {items_name} to be migrated.")
    print()

    print("Possible options:")
    print(", ".join(options))
    print()

    print("1. Select all")
    print("2. Select subset")
    print("3. Exclude subset")
    print()

    while True:
        ans = input("? ")
        if ans in ("1", "2", "3"):
            break
        else:
            print("Type 1, 2 or 3")
    
    if ans == "1":
        return options
    else:
        while True:
            subset_str = input(
                f"Input the items to {'include' if ans == '2' else 'exclude'} "
                "separated with commas:\n"
            )

            subset = set(subset_str.replace(",", " ").split())

            if not subset.issubset(set(options)):
                print(f"ERROR. Unknown items: {subset.difference(set(options))}")
            else:
                break
        
        if ans == "2":
            return list(subset)
        elif ans == "3":
            return list(set(options).difference(subset))
        else:
            raise RuntimeError

def get_table_names(psycopg_conn,
    postgresql_database, postgresql_password, postgresql_host, postgresql_port, o_file='file.txt') -> List[str]:

    q = f'psql postgresql://{postgresql_database}:{postgresql_password}@{postgresql_host}:{postgresql_port} -c "\d ;" -o {o_file}'
    os.system(q)
    names = []

    with open(o_file, 'rt') as f:
        lines = f.readlines()
        for line in lines[3:]: 
            res_cols = line.split('|')
            if len(res_cols) < 3:
                continue
            schema_name = res_cols[0].strip()
            if schema_name != 'public':
                continue
            cur_table_name = res_cols[1].strip()
            names.append(cur_table_name)

    return names

def get_column_names(psycopg_conn, table_name: str,
    postgresql_database, postgresql_password, postgresql_host, postgresql_port, o_file='file.txt') -> List[str]:
    q = f'psql postgresql://{postgresql_database}:{postgresql_password}@{postgresql_host}:{postgresql_port} -c "\d+ {table_name};" -o {o_file}'
    os.system(q)
    names = []
    with open(o_file, 'rt') as f:
        lines = f.readlines()
        for line in lines[3:]: 
            res_cols = line.split('|')
            if len(res_cols) < 4:
                continue
            col_name = res_cols[0].strip()
            names.append(col_name)

    return names

def generate_create_table_statement(psycopg_conn, table_name: str, columns: List[str],
    postgresql_database, postgresql_password, postgresql_host, postgresql_port, o_file='file.txt') -> str:
    q = f'psql postgresql://{postgresql_database}:{postgresql_password}@{postgresql_host}:{postgresql_port} -c "\d+ {table_name};" -o {o_file}'
    os.system(q)
    result_columns = []
    with open(o_file, 'rt') as f:
        lines = f.readlines()
        for line in lines[3:]: 
            res_cols = line.split('|')
            if len(res_cols) < 4:
                continue
            col_name = res_cols[0].strip()
            if col_name not in columns:
                continue
            data_type = res_cols[1].strip()
            is_not_null = res_cols[3].strip()
            result_columns.append(' '.join((col_name, data_type, is_not_null)))
            
    final_statement = f'''
        CREATE OR REPLACE TABLE {table_name} (
            {', '.join(result_columns)}
        );
    '''    
    return final_statement

def linpy_execute_and_commit(linpy_conn, cmd):
    linpy_cursor = linpy_conn.cursor()
    linpy_cursor.execute(cmd)
    linpy_cursor.commit()
    linpy_cursor.close()


def my_iter(psycopg_conn, table_name: str, columns: List[str], batch_size: int = 2):
    cursor = psycopg_conn.cursor()
    cursor.execute(f"SELECT {', '.join(columns)} FROM {table_name};")
    python_none = 'None'
    sql_none = 'null'
    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break

        sql_insert = ""
        sql_insert += f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES \n"
        for row_ix, row in enumerate(rows):
            # row_str = str(row).replace(python_none, sql_none)
            row_str =  re.sub(r"(^\(|, )(None)(, '|\)$|,\)$)", r"\1NULL\3", str(row))
        
            if row_str.endswith(",)"):
                row_str = row_str[:-2] + ")"

            sql_insert += row_str
             
            if row_ix < len(rows) - 1:
                sql_insert += ",\n"
            else:
                sql_insert += ";"

        yield sql_insert
    
    cursor.close()

def process_nulls(psycopg_conn, table_name: str, columns: List[str]):
    cursor = psycopg_conn.cursor()
    q = 'select\n'
    # print(columns)
    for idx, col in enumerate(columns):
        q += f'\tsum(case when {col} is null then 1 else 0 end) cnt_nulls_for_{col}'
        # print('>>>>>>>>>', idx, len(columns))
        if idx != len(columns) - 1:
            q += ','
        q += '\n'        
    
    q += f'from\n\t{table_name}'
    # print(q)
    cursor.execute(q)
    result = cursor.fetchall()[0]
    for idx, col in enumerate(columns):
        if result[idx] > 0:
            with open('log_migration.txt', 'a') as f:
                f.write(f'THERE ARE {result[idx]} NULL VALUES IN {col} COLUMN IN TABLE {table_name}!\n')
    # print(result)
    cursor.close()

def migrate_table(psycopg_conn, linpy_conn, table_name: str, columns: List[str],
        postgresql_database, postgresql_password, postgresql_host, postgresql_port):
    process_nulls(psycopg_conn, table_name, columns)

    create_table_statement = generate_create_table_statement(psycopg_conn, table_name, columns, 
        postgresql_database, postgresql_password, postgresql_host, postgresql_port)

    linpy_execute_and_commit(linpy_conn=linpy_conn, cmd=create_table_statement)

    for sql_insert in my_iter(
        psycopg_conn=psycopg_conn,
        table_name=table_name,
        columns=columns,
    ):
        # print(sql_insert)
        linpy_execute_and_commit(linpy_conn=linpy_conn, cmd=sql_insert)


def migrate(psycopg_conn, linpy_conn, postgresql_database, postgresql_password, postgresql_host, postgresql_port):
    tables = get_table_names(psycopg_conn, 
        postgresql_database, postgresql_password, postgresql_host, postgresql_port)
    tables = ask_user_to_select(items_name="tables", options=tables)

    for table in tables:
        print()
        print("=" * 10)
        print(f"Starting migration for {table} table.")

        columns = get_column_names(psycopg_conn, table,
            postgresql_database, postgresql_password, postgresql_host, postgresql_port)
        columns = ask_user_to_select(items_name="columns", options=columns)
        
        migrate_table(psycopg_conn, linpy_conn, table, columns, postgresql_database, postgresql_password, postgresql_host, postgresql_port)

        print(f"Migration for {table} table is successful.")
        print("=" * 10)


@click.command()
@click.option("--postgresql_host", type=str, default="localhost")
@click.option("--postgresql_database", type=str, default="postgres")
@click.option("--postgresql_user", type=str, default="postgres")
@click.option("--postgresql_password", type=str, default="testtest")
@click.option("--postgresql_port", type=int, default=5432)
@click.option("--linpy_user", type=str, default="SYSTEM")
@click.option("--linpy_password", type=str, default="MANAGER")
def main(
    postgresql_host: str,
    postgresql_database: str,
    postgresql_user: str,
    postgresql_password: str,
    postgresql_port: int,
    linpy_user: str,
    linpy_password: str,
):
    finished = False

    while not finished:
        try:
            with open('log_migration.txt', 'w') as f:
                pass
            psycopg_conn = psycopg2.connect(
                host=postgresql_host,
                database=postgresql_database,
                user=postgresql_user,
                password=postgresql_password,
                port=str(postgresql_port)
            )

            linpy_conn = LinPy.connect(
                user=linpy_user,
                password=linpy_password,
            )

            migrate(psycopg_conn, linpy_conn, postgresql_database, postgresql_password, postgresql_host, postgresql_port)
            finished = True
        except (psycopg2.errors.Error, LinPy.DatabaseError) as err:
            print("ERROR. Database error:")
            print(err)
            print()

            finished = not ask_yes_no("Try again")
        finally:
            try:
                psycopg_conn.close()
                linpy_conn.close()
            except:
                pass

if __name__ == "__main__":
    main()
