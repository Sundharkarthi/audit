from django.shortcuts import render

# Create your views here.
from openpyxl.workbook import Workbook

import pandas as pd
import snowflake.connector
from rest_framework.views import APIView
from rest_framework.response import Response


class Audit(APIView):

    def post(self, request, format=None):
        name = request.data["user"]
        userpwd = request.data["password"]
        database = request.data["database"]

        account = request.data["account"]
        warehouse = request.data["warehouse"]
        schema = request.data["schema"]
        role = request.data["role"]

        def getColumnDtypes(dataTypes):

            dataList = []
            for x in dataTypes:
                if (x == 'NUMBER'):
                    dataList.append('int')
                elif (x == 'FLOAT'):
                    dataList.append('float')
                elif (x == 'bool'):
                    dataList.append('boolean')
                elif (x == 'DATE'):
                    dataList.append('DATE')
                elif (x == 'TIMESTAMP_LTZ'):
                    dataList.append('DATE')
                else:
                    dataList.append('varchar')

            return dataList

        url = snowflake.connector.connect(
            user=name,
            password=userpwd,
            account=account,
            warehouse=warehouse,
            database=database,
            schema=schema,
            role=role
        )

        cursor = url.cursor()
        script = """
    select a.TABLE_NAME,a.TABLE_SCHEMA,a.TABLE_CATALOG,b.COLUMN_NAME ,b.DATA_TYPE,b.ORDINAL_POSITION,jim.ROW_COUNT from snowflake.account_usage.tables a 
     left join INFORMATION_SCHEMA.columns b on(a.TABLE_NAME=b.TABLE_NAME)
    left join INFORMATION_SCHEMA.tables jim on (b.TABLE_NAME = jim.TABLE_NAME) ;

    """

        cursor.execute(script)

        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()

        df = pd.DataFrame(list(data), columns=columns)
        dummy = getColumnDtypes(df['DATA_TYPE'].tolist())
        # df['target_data_type']= pd.dummy

        df['TARGET_DATA_TYPE'] = dummy
        df['SOURCE'] = 'snowflake'
        df['TARGET'] = 'oracle'
        writer = pd.ExcelWriter('audit_report4.xlsx')
        df.to_excel(writer, sheet_name='bar')
        writer.save()

