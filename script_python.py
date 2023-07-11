# %%
import json
import pyodbc
import pandas as pd
from sqlalchemy import create_engine, select, MetaData, Table
import sqlalchemy as sa
import urllib

# %%
def getAuthforWMS(f_data):
    wmsAccess = f_data["wmsAccess"][0]
    return wmsAccess

# %%
def getConnforMYSQL(f_data, accessType):
    dialect = pyodbc.drivers()[-1]
    server = f_data[accessType][0]["server"]
    db = f_data[accessType][0]["database"]
    uid = f_data[accessType][0]["uid"]
    pwd = f_data[accessType][0]["pwd"]
    driver = f_data[accessType][0]["dialect_driver"]
    port = f_data[accessType][0]["port"]

    if accessType == "azureAccess":
        connection_string = (
            " Driver={%s}" %dialect +
            "; SERVER=%s" %server + 
            "; Database=%s " %db + 
            "; UID=%s" %uid +
            "; PWD=%s" %pwd
        )
        quoted = urllib.parse.quote_plus(connection_string)
        quoted = f_data[accessType][0]["dialect_driver"] + quoted
        engine = create_engine(quoted, fast_executemany=True).execution_options(isolation_level="AUTOCOMMIT")
    else:
        quoted = driver + uid + ":" + pwd + "@" + server + ":" + str(port) + "/" + db
        engine = create_engine(quoted).execution_options(isolation_level="AUTOCOMMIT")
        
    return engine

# %%
#data extraction
def extraction_function(conn_integrator, sql_text):
    with conn_integrator.begin():
        str_sql = sa.text(sql_text)
        results = conn_integrator.execute(str_sql)
        columns = results.keys()

        list_columns = []
        list_rows = []
        #print column_names
        for column_name in columns:
            list_columns += [column_name]
        for row in results:
            list_rows += [row]

    return pd.DataFrame(list_rows, columns = list_columns)

# %%
def main ():
    #open auth file
    auth = open('auth.json')
    auth_load = json.load(auth)

    #getting conn for 
    engine_integrator = getConnforMYSQL(auth_load, "integratorAccess")
    conn_integrator = engine_integrator.connect()

    #getting conn for azure
    engine_azure = getConnforMYSQL(auth_load, "azureAccess")
    conn_azure = engine_azure.connect()

    #get functions file
    functions = open ('extract_data.json')
    functions_load = json.load(functions)

    #do_stuff
    for key in functions_load:
        for work in functions_load[key]:
            #extract data from integretor
            sql_text = work["sql_text"] + "FROM " + work["org_table"]
            df = extraction_function(conn_integrator, sql_text)
            
            #to-do: clear azure table
            conn_azure.execute("DELETE FROM dbo." + work["tgt_table"])
            
            #load data to azure table
            df.to_sql(work["tgt_table"], con=engine_azure, if_exists='replace', index=False)
            
    conn_integrator.close()
    conn_azure.close()


# %%
if __name__ == "__main__":
    main()
    print('done')


