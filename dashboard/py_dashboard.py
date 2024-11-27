from db_connection import py_connection
from datetime import datetime as dt, timedelta

def sales_type_chart(request, decoded):  # V1 -----> sales type chart Api starts here
    try:
        config_details = get_table_config_details(request, decoded)
        if config_details and len(config_details) > 0:
            res, bills = get_sales_type_chart(config_details[0][0], config_details[0][1], request, decoded)
            return {"type": res, "no.of_bills": bills}
        else:
            return {"type": [], "no.of_bills": []}

    except Exception as e:
        print(str(e))
        return {"type": [], "no.of_bills": []}

def get_table_config_details(request, decoded):
    try:
        qry = ("select report_table,report_config_fk from Reporting.main_menu where comp_fk='{0}' and "
               "menupk='{1}'").format(decoded['comp_fk'], request['menu_pk'])
        res = py_connection.get_result(qry)
        if res and len(res) > 0:
            return res
        else:
            return res
    except Exception as e:
        print(str(e))
        return []

def get_sales_type_chart(report_table, report_config_fk, request, decoded):
    try:
        start_dt = str(request['start_date']) + " 00:00:00"
        end_dt = str(request['end_date']) + " 23:59:59"

        qry = "select sales_type, main_table, bills from Reporting." + str(
            report_table) + " where report_config_pk =" + str(report_config_fk)
        res = py_connection.get_result(qry)
        if res and len(res) > 0:
            sales = get_sales_type_chart_details(res[0][0], decoded, start_dt, end_dt)
            bills = get_bills(res[0][1], res[0][2], start_dt, end_dt, decoded)
            return sales, bills
        else:
            return []
    except Exception as e:
        print(str(e))
        return []

def get_sales_type_chart_details(procedure_name,  decoded, start_dt, end_dt):
    try:
        # start_dt = '2024-04-01 00:00:00'
        # end_dt = '2024-04-03 23:59:59'
        qry = '{call Reporting.' + str(procedure_name) + '(?,?,?)}'
        res, k = py_connection.call_prop1(qry, (start_dt, end_dt, decoded['comp_fk']))
        lst = []
        if res and len(res) > 0:
            for row in res:
                view_data = dict(zip(k, row))
                lst.append(view_data)
            return lst
        else:
            return lst
    except Exception as e:
        print(str(e))
        return []


def get_bills(table, procedure_name, start_dt, end_dt, decoded):
    try:

        qry = '{call Reporting.' + str(procedure_name) + '(?,?,?,?)}'
        res, k = py_connection.call_prop1(qry, (start_dt, end_dt, decoded['comp_fk'], table))
        lst = []
        if res and len(res) > 0:
            for row in res:
                view_data = dict(zip(k, row))
                lst.append(view_data)
            return lst
        else:
            return lst
    except Exception as e:
        print(str(e))
        return []

# ---------------------------------------------------> sales document type chart Api --------------->

def sales_document_type_chart(request, decoded):  # V1 -----> sales document type chart Api starts here
    try:
        config_details = get_table_config_details(request, decoded)
        if config_details and len(config_details) > 0:
            res = get_sales_document_type_chart(config_details[0][0], config_details[0][1], request, decoded)
            return {"document_type": res}
        else:
            return {"document_type": []}

    except Exception as e:
        print(str(e))
        return {"document_type": []}

def get_sales_document_type_chart(report_table, report_config_fk, request, decoded):
    try:
        qry = "select sales_document_type from Reporting." + str(
            report_table) + " where report_config_pk =" + str(report_config_fk)
        res = py_connection.get_result(qry)
        if res and len(res) > 0:
            sales = get_sales_document_type_chart_details(res[0][0], request, decoded)
            return sales
        else:
            return []
    except Exception as e:
        print(str(e))
        return []


def get_sales_document_type_chart_details(procedure_name, request, decoded):
    try:
        start_dt = str(request['start_date']) + " 00:00:00"
        end_dt = str(request['end_date']) + " 23:59:59"
        # start_dt = '2024-04-01 00:00:00'
        # end_dt = '2024-04-03 23:59:59'
        qry = '{call Reporting.' + str(procedure_name) + '(?,?,?)}'
        res, k = py_connection.call_prop1(qry, (start_dt, end_dt, decoded['comp_fk']))
        lst = []
        if res and len(res) > 0:
            for row in res:
                view_data = dict(zip(k, row))
                lst.append(view_data)
            return lst
        else:
            return lst
    except Exception as e:
        print(str(e))
        return []


# --------------------------------------> Sales Customer Chart Api ---------------------->

def sales_customer_chart(request, decoded):  # V1 -----> sales customer chart Api starts here
    try:
        config_details = get_table_config_details(request, decoded)
        if config_details and len(config_details) > 0:
            res = get_sales_customer_chart(config_details[0][0], config_details[0][1], request, decoded)
            return {"customer_chart": res}
        else:
            return {"customer_chart": []}

    except Exception as e:
        print(str(e))
        return {"customer_chart": []}

def get_sales_customer_chart(report_table, report_config_fk, request, decoded):
    try:
        qry = "select sales_customer_chart from Reporting." + str(
            report_table) + " where report_config_pk =" + str(report_config_fk)
        res = py_connection.get_result(qry)
        if res and len(res) > 0:
            sales = get_sales_customer_chart_details(res[0][0], request, decoded)
            return sales
        else:
            return []
    except Exception as e:
        print(str(e))
        return []


def get_sales_customer_chart_details(procedure_name, request, decoded):
    try:
        start_dt = str(request['start_date']) + " 00:00:00"
        end_dt = str(request['end_date']) + " 23:59:59"
        Type = request['type']
        # start_dt = '2024-09-06 00:00:00'
        # end_dt = '2024-09-06 23:59:59'
        # Type = 'All'
        qry = '{call Reporting.' + str(procedure_name) + '(?,?,?,?)}'
        res, k = py_connection.call_prop1(qry, (Type, start_dt, end_dt, decoded['comp_fk']))
        lst = []
        if res and len(res) > 0:
            for row in res:
                view_data = dict(zip(k, row))
                lst.append(view_data)
            return lst
        else:
            return lst
    except Exception as e:
        print(str(e))
        return []