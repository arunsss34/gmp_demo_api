import uuid
from db_connection import py_connection
from collections import defaultdict
from report.customer_popdf import generate_pdf
from datetime import datetime as dt
from generate_pdf.customer_po_pdf_lw import generate_pdf1


def get_customer_po_pdf(request, decoded):
    try:
        date = request['date']
        qry = '{call [Reporting].[customer_pending_order_details] (?,?)}'
        res, k = py_connection.call_prop1(qry, (str(format_date(date)), decoded['comp_fk']))
        lst = []
        if res and len(res) > 0:
            for row in res:
                view_data = dict(zip(k, row))
                lst.append(view_data)
            grouped_data = defaultdict(list)

            for entry in lst:
                item_desc = entry.pop('ItemDescription')
                grouped_data[item_desc].append(entry)

            # Converting to the desired structure
            formatted_result = []
            for item_desc, details in grouped_data.items():
                formatted_result.append({
                    'item_description': item_desc,
                    'data': details
                })

            return {"base64string": generate_pdf(formatted_result, str(date)), "file_name": "Lucky Yarn Customer PO Details.pdf"}
        else:
            return {"base64string": '', "file_name": ''}
    except Exception as e:
        print(str(e))
        return {"base64string": '', "file_name": ''}

def format_date(date):
    try:
        date_str = str(date)
        date_obj = dt.strptime(date_str, '%d-%m-%Y')
        formatted_date = date_obj.strftime('%Y-%m-%d')
        return formatted_date
    except Exception as e:
        print(str(e))

# Lucky Yarns Customer PO

def get_customer_po_details(request, decoded):
    try:
        config_details = get_table_config_details(request, decoded)
        if config_details and len(config_details) > 0:
            details = get_details(config_details[0][0], config_details[0][1], request, decoded)
            return {"details": details[0], "fields": details[1],
                    "last_updated_date": last_updated_date_for_customer_po(decoded)}
    except Exception as e:
        print(str(e))
        return {"details": [], "fields": [],
                "last_updated_date": last_updated_date_for_customer_po(decoded)}


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

def get_details(report_table, report_config_fk, request, decoded):
    try:
        qry = "select customer_po_rpt from Reporting."+ str(report_table) + " where report_config_pk =" + str(report_config_fk)
        res = py_connection.get_result(qry)
        if res and len(res) > 0:
            customer_po_details = get_customer_po_table_rpt(res[0][0], request, decoded)
            return customer_po_details
        else:
            return []
    except Exception as e:
        print(str(e))
        return []


def get_customer_po_table_rpt(procedure_name, request, decoded):
    try:
        # date = '2024-09-14'
        date = request['date']
        qry = '{call Reporting.' + str(procedure_name) + '(?,?)}'
        res, k = py_connection.call_prop1(qry, (str(format_date(date)), decoded['comp_fk']))
        # result = {}
        # total = {}
        lst = []
        if res and len(res) > 0:
            # last_values = {}  # Store the last row's values for each Type

            for row in res:
                view_data = dict(zip(k, row))
                lst.append(view_data)
            #     row_dict = {k[i]: row[i] for i in range(len(k))}
            #     Type = row_dict.get('ItemDescription')
            #
            #     if Type not in result:
            #         result[Type] = []
            #
            #     # Create item details excluding the 'Type'
            #     item_details = {key: value for key, value in row_dict.items() if
            #                     key not in ['ItemDescription', 'Subtotal Bags', 'Subtotal PendingBags',
            #                                 'Total Bags', 'Total PendingBags']}
            #
            #
            #     result[Type].append(item_details)
            #
            #     # Store the last row's values for each Type
            #     last_values[Type] = {
            #         k[6]: row_dict.get("Subtotal Bags", 0),
            #         k[8]: row_dict.get("Subtotal PendingBags", 0)
            #     }
            #
            #     total = {
            #         k[1]: '',
            #         k[2]: '',
            #         k[3]: '',
            #         k[4]: 'Over All Total',
            #         # k[5]: '',
            #         k[6]: float(row_dict.get("Total Bags", 0)),
            #         k[7]: '',
            #         k[8]: float(row_dict.get("Total PendingBags", 0))
            #     }
            #
            # # Now append the subtotal for each Type using the last row's values
            # for Type in last_values:
            #     subtotal = {
            #         k[1]: '',
            #         k[2]: '',
            #         k[3]: '',
            #         k[4]: 'Total',
            #         # k[5]: '',
            #         k[6]: float(last_values[Type][k[6]]),
            #         k[7]: '',
            #         k[8]: float(last_values[Type][k[8]])
            #     }
            #     result[Type].append(subtotal)
            #     total.update(total)
            #
            # k = [item for item in k if item not in ['ItemDescription', 'Subtotal Bags', 'Subtotal PendingBags',
            #                                         'Total Bags', 'Total PendingBags']]

            return lst, k
        else:
            return [], k
    except Exception as e:
        print(str(e))
        return '', '', ''

def last_updated_date_for_customer_po(decoded):
    qry = ("select  top 1 format(UpdatedDate, 'dd-MM-yyyy') as last_updated_date from RptSPG.PendingOrderReport where "
           "CompanyID =" + str(decoded['comp_fk']) + " order by UpdatedDate desc")
    res = py_connection.get_result(qry)
    return res[0][0]


# Lucky Weaves Customer PO
def get_customer_po_details_lw(request, decoded):
    try:
        config_details = get_table_config_details(request, decoded)
        if config_details and len(config_details) > 0:
            details = get_details_lw(config_details[0][0], config_details[0][1], request, decoded)
            return {"details": details[0], "fields": details[1],
                    "last_updated_date": last_updated_date_for_customer_po_lw1()}
    except Exception as e:
        print(str(e))
        return {"details": [], "fields": [],
                "last_updated_date": last_updated_date_for_customer_po_lw1()}

def get_details_lw(report_table, report_config_fk, request, decoded):
    try:
        qry = "select customer_po_rpt from Reporting."+ str(report_table) + " where report_config_pk =" + str(report_config_fk)
        res = py_connection.get_result(qry)
        if res and len(res) > 0:
            customer_po_details = get_customer_po_table_rpt_lw(res[0][0], request, decoded)
            return customer_po_details
        else:
            return []
    except Exception as e:
        print(str(e))
        return []


def get_customer_po_table_rpt_lw(procedure_name, request, decoded):
    try:
        # date = '2024-09-14'
        date = request['date']
        qry = '{call Reporting.' + str(procedure_name) + '(?,?)}'
        res, k = py_connection.call_prop1(qry, (str(format_date(date)), decoded['comp_fk']))
        # result = {}
        # total = {}
        lst = []
        if res and len(res) > 0:
            # last_values = {}  # Store the last row's values for each Type

            for row in res:
                view_data = dict(zip(k, row))
                lst.append(view_data)
            #     row_dict = {k[i]: row[i] for i in range(len(k))}
            #     Type = row_dict.get('ItemDescription')
            #
            #     if Type not in result:
            #         result[Type] = []
            #
            #     # Create item details excluding the 'Type'
            #     item_details = {key: value for key, value in row_dict.items() if
            #                     key not in ['ItemDescription', 'Subtotal Bags', 'Subtotal PendingBags',
            #                                 'Total Bags', 'Total PendingBags']}
            #
            #
            #     result[Type].append(item_details)
            #
            #     # Store the last row's values for each Type
            #     # last_values[Type] = {
            #     #     k[6]: row_dict.get("Subtotal Bags", 0),
            #     #     k[8]: row_dict.get("Subtotal PendingBags", 0)
            #     # }
            #
            #     total = {
            #         k[1]: '',
            #         k[2]: '',
            #         k[3]: '',
            #         k[4]: 'Over All Total',
            #         # k[5]: '',
            #         k[6]: float(row_dict.get("Total Bags", 0)),
            #         k[7]: '',
            #         k[8]: float(row_dict.get("Total PendingBags", 0))
            #     }
            #
            # # Now append the subtotal for each Type using the last row's values
            # for Type in last_values:
            #     subtotal = {
            #         k[1]: '',
            #         k[2]: '',
            #         k[3]: '',
            #         k[4]: 'Total',
            #         # k[5]: '',
            #         k[6]: float(last_values[Type][k[6]]),
            #         k[7]: '',
            #         k[8]: float(last_values[Type][k[8]])
            #     }
            #     result[Type].append(subtotal)
            #     total.update(total)

            k = [item for item in k if item not in ['ItemDescription', 'Subtotal Bags', 'Subtotal PendingBags',
                                                    'Total Bags', 'Total PendingBags']]

            return lst, k
        else:
            return [], k
    except Exception as e:
        print(str(e))
        return '', ''

def last_updated_date_for_customer_po_lw1():
    qry = ("select  top 1 format(UpdatedDate, 'dd-MM-yyyy') as last_updated_date from RptSPG.PendingOrderReport where "
           "CompanyID = 2 order by UpdatedDate desc")
    res = py_connection.get_result(qry)
    return res[0][0]


def get_customer_po_pdf_lw(request, decoded):
    try:
        date = request['date']
        qry = '{call [Reporting].[lucky_weaves_customer_pending_order_details] (?,?)}'
        res, k = py_connection.call_prop1(qry, (str(format_date(date)), decoded['comp_fk']))
        lst = []
        if res and len(res) > 0:
            for row in res:
                view_data = dict(zip(k, row))
                lst.append(view_data)
            grouped_data = defaultdict(list)
            for entry in lst:
                item_desc = entry.pop('ItemDescription')
                grouped_data[item_desc].append(entry)

            # Converting to the desired structure
            formatted_result = []
            for item_desc, details in grouped_data.items():
                formatted_result.append({
                    'item_description': item_desc,
                    'data': details
                })
            return {"base64string": generate_pdf1(formatted_result, str(date)), "file_name": "Lucky Weaves Customer PO Details.pdf"}
        else:
            return {"base64string": '', "file_name": ''}
    except Exception as e:
        print(str(e))
        return {"base64string": '', "file_name": ''}