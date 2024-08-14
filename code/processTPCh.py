import argparse 
import psycopg2 
import os

def SetupDatabase(database_name): 
    try: 
        connection = psycopg2.connect(user="postgres", password="Aa123456", host="localhost", port="5432", database=database_name)

        cursor = connection.cursor()

    # SQL commands to create tables
        table_queries = ['''CREATE TABLE NATION  ( N_NATIONKEY  INTEGER NOT NULL,
                            N_NAME       CHAR(25) NOT NULL,
                            N_REGIONKEY  INTEGER NOT NULL,
                            N_COMMENT    VARCHAR(152));''',

                        '''CREATE TABLE REGION  ( R_REGIONKEY  INTEGER NOT NULL,
                            R_NAME       CHAR(25) NOT NULL,
                            R_COMMENT    VARCHAR(152));''',

                        '''-- For table REGION
                            ALTER TABLE REGION
                                ADD PRIMARY KEY (R_REGIONKEY);

                            -- For table NATION
                            ALTER TABLE NATION
                                ADD PRIMARY KEY (N_NATIONKEY);

                            ALTER TABLE NATION
                                ADD CONSTRAINT NATION_FK1
                                FOREIGN KEY (N_REGIONKEY) references REGION;

                            -- For table PART
                            ALTER TABLE PART
                                ADD PRIMARY KEY (P_PARTKEY);

                            -- For table SUPPLIER
                            ALTER TABLE SUPPLIER
                                ADD PRIMARY KEY (S_SUPPKEY);

                            ALTER TABLE SUPPLIER
                                ADD CONSTRAINT SUPPLIER_FK1
                                FOREIGN KEY (S_NATIONKEY) references NATION;

                            -- For table PARTSUPP
                            ALTER TABLE PARTSUPP
                                ADD PRIMARY KEY (PS_PARTKEY,PS_SUPPKEY);

                            -- For table CUSTOMER
                            ALTER TABLE CUSTOMER
                                ADD PRIMARY KEY (C_CUSTKEY);

                            ALTER TABLE CUSTOMER
                                ADD CONSTRAINT CUSTOMER_FK1
                                FOREIGN KEY (C_NATIONKEY) references NATION;

                            -- For table LINEITEM
                            ALTER TABLE LINEITEM
                                ADD PRIMARY KEY (L_ORDERKEY,L_LINENUMBER);

                            -- For table ORDERS
                            ALTER TABLE ORDERS
                                ADD PRIMARY KEY (O_ORDERKEY);

                            -- For table PARTSUPP
                            ALTER TABLE PARTSUPP
                                ADD CONSTRAINT PARTSUPP_FK1
                                FOREIGN KEY (PS_SUPPKEY) references SUPPLIER;

                            ALTER TABLE PARTSUPP
                                ADD CONSTRAINT PARTSUPP_FK2
                                FOREIGN KEY (PS_PARTKEY) references PART;

                            -- For table ORDERS
                            ALTER TABLE ORDERS
                                ADD CONSTRAINT ORDERS_FK1
                                FOREIGN KEY (O_CUSTKEY) references CUSTOMER;

                            -- For table LINEITEM
                            ALTER TABLE LINEITEM
                                ADD CONSTRAINT LINEITEM_FK1
                                FOREIGN KEY (L_ORDERKEY) references ORDERS;

                            ALTER TABLE LINEITEM
                                ADD CONSTRAINT LINEITEM_FK2
                                FOREIGN KEY (L_PARTKEY,L_SUPPKEY) references PARTSUPP;
                        '''
                        ]


    # Execute table creation queries
        for query in table_queries:
            cursor.execute(query)
        print("Tables Created")
        connection.commit()
        connection.close()

    except (Exception, psycopg2.Error) as error:
        print(error)

def AddKeysToTables(database_name): 
    try: 
        connection = psycopg2.connect(user="postgres", password="Aa123456", host="localhost", port="5432", database=database_name)

        cursor = connection.cursor()

        # SQL commands to add keys to tables
        key_queries = ['''ALTER TABLE NATION ADD PRIMARY KEY (N_NATIONKEY);

                        ALTER TABLE NATION ADD CONSTRAINT NATION_FK1 FOREIGN KEY (N_REGIONKEY) references REGION;

                        ALTER TABLE PART ADD PRIMARY KEY (P_PARTKEY);

                        ALTER TABLE SUPPLIER ADD PRIMARY KEY (S_SUPPKEY);

                        ALTER TABLE SUPPLIER ADD CONSTRAINT SUPPLIER_FK1 FOREIGN KEY (S_NATIONKEY) references NATION;

                        ALTER TABLE PARTSUPP ADD PRIMARY KEY (PS_PARTKEY,PS_SUPPKEY);

                        ALTER TABLE CUSTOMER ADD PRIMARY KEY (C_CUSTKEY);

                        ALTER TABLE CUSTOMER ADD CONSTRAINT CUSTOMER_FK1 FOREIGN KEY (C_NATIONKEY) references NATION;

                        ALTER TABLE LINEITEM ADD PRIMARY KEY (L_ORDERKEY,L_LINENUMBER);

                        ALTER TABLE ORDERS ADD PRIMARY KEY (O_ORDERKEY);

                        ALTER TABLE PARTSUPP ADD CONSTRAINT PARTSUPP_FK1 FOREIGN KEY (PS_SUPPKEY) references SUPPLIER;

                        ALTER TABLE PARTSUPP ADD CONSTRAINT PARTSUPP_FK2 FOREIGN KEY (PS_PARTKEY) references PART;

                        ALTER TABLE ORDERS ADD CONSTRAINT ORDERS_FK1 FOREIGN KEY (O_CUSTKEY) references CUSTOMER;

                        ALTER TABLE LINEITEM ADD CONSTRAINT LINEITEM_FK1 FOREIGN KEY (L_ORDERKEY) references ORDERS;

                        ALTER TABLE LINEITEM ADD CONSTRAINT LINEITEM_FK2 FOREIGN KEY (L_PARTKEY,L_SUPPKEY) references PARTSUPP;
                        ''']

        # Execute key addition queries
        for query in key_queries:
            cursor.execute(query)
        print("Key Created")

        connection.commit()
        connection.close()

    except (Exception, psycopg2.Error) as error:
        print(error)

def AddDataToTables(database_name, data_scale): 
    try: 
        connection = psycopg2.connect(user="postgres", password="Aa123456", host="localhost", port="5432", database=database_name)
        cursor = connection.cursor()

        # SQL commands to add data to tables
        relations = ["REGION", "NATION"]  # Add more table names here
        for table_name in relations:
            file_path =  'Data/_' + data_scale + '/' + table_name.lower() +".csv"
            data = open(file_path, 'r')
            cursor.copy_from(data, table_name.lower(), sep='|')

        print("Data Read")
        connection.commit()
        connection.close()

    except (Exception, psycopg2.Error) as error:
        print(error)

def data_cleaning(database):
    try:
        connection = psycopg2.connect(user="postgres",
                                port="5432",
                                database= database)

        cursor = connection.cursor()
        
        # SQL query drop tables
        relations = ["REGION", "NATION", "SUPPLIER", "CUSTOMER", "PART", "PARTSUPP", "ORDERS", "LINEITEM"]
        for element in reversed(relations):
            command = "DROP TABLE " + element.lower() + ";"
            cursor.execute(command)
        print("Cleaned Table")
        connection.commit()
        connection.close()     
    except (Exception, psycopg2.Error) as error:
        print(error)


def main(): 
    parser = argparse.ArgumentParser(description="table") 
    parser.add_argument("database_name", type=str, help="database") 
    parser.add_argument("data_scale", type=str, help="size") 
    args = parser.parse_args()

    SetupDatabase(args.database_name)
    AddKeysToTables(args.database_name)
    AddDataToTables(args.database_name, args.data_scale)
    data_cleaning(args.database_name)
    
    if __name__ == "__main__":
        main()