from db_connection import py_connection
from datetime import datetime as dt, timedelta
from collections import defaultdict


def get_sales_document_type(request, decoded):
    try:
        print(request, "1111")
        year = str(dt.now().year)
        start_date1 = (str(year) + '-' + str(request['start_date']))
        print(start_date1, "00000")
        start_date = dt.strptime(start_date1, '%Y-%m-%d')
        print(start_date, "---------99")
        end_date1 = str(year) + '-' + str(request['end_date'])
        print(end_date1,"11111111")
        end_date = handle_leap_year_adjustment(end_date1)
        print(end_date,"222222")

        start_month, end_month = get_months(start_date, end_date)
        if start_month == 1 and end_month == 12:
            qry = '{call [Reporting].[Sample_sales_Analysis] (?,?,?,?,?)}'
            params = (0, 0, decoded['comp_fk'], year, 0)
        elif start_month == end_month:
            qry = '{call [Reporting].[Sample_sales_Analysis] (?,?,?,?,?)}'
            params = (0, end_month, decoded['comp_fk'], year, 0)
        else:
            qry = '{call [Reporting].[Sample_sales_Analysis] (?,?,?,?,?)}'
            params = (start_month, end_month, decoded['comp_fk'], year, 1)
        res, k = py_connection.call_prop1(qry, params)
        lst = []
        # result_dict = defaultdict(lambda: defaultdict(lambda: None))

        if res and len(res) > 0:
            for row in res:
                view_data = dict(zip(k, row))
                lst.append(view_data)
            print(lst,"000")
            return {"data": lst}
        #         if start_month == end_month:
        #             view_data[str(end_month)] = view_data.pop('Month', 'None')
        #
        #         month_keys = [key for key in view_data if key.isdigit()]
        #         for month in month_keys:
        #             amount = view_data.get(month, 0)
        #             result_dict[month][view_data['DocumentType']] = None if amount == 0 else amount
        #
        #     for month, data in result_dict.items():
        #         result_dict[month] = {k: (0 if v is None else v) for k, v in data.items()}
        #
        #     formatted_result = [{month: data} for month, data in result_dict.items()]
        #
        #
        #     print({'data': formatted_result})
        #     return {"data": formatted_result}
        else:
            return {"data": []}
    except Exception as e:
        print(str(e))


def handle_leap_year_adjustment(end_date):
    try:
        end_dt = dt.strptime(end_date, '%Y-%m-%d')
        if end_dt.month == 2 and end_dt.day == 28:
            next_day = end_dt + timedelta(days=1)
            return next_day.strftime('%Y-%m-%d')
        return end_dt
    except Exception as e:
        print(str(e))
        return end_date

def get_months(start_date, end_date):
    try:
        print(start_date, end_date,"222")
        print(start_date.month,"22244")
        print(end_date.month)
        return start_date.month, end_date.month
    except Exception as e:
        print(str(e))
        return None, None

# --------------------------------------------------------------------->  SALES Analysis CHART

def get_sales_chart(request, decoded):
    try:
        print(request, "1111")
        current_year = dt.now().year
        # current_month = dt.now().month
        today = dt.today()
        if request['value'] == 'year':
            start_date = dt(current_year, 1, 1).date()
            end_date1 = dt(current_year + 1, 1, 1).date()
            end_date = end_date1 - timedelta(days=1)
        elif request['value'] == 'month':
            start_date = today.replace(day=1)
            end_date = today
        elif request['value'] == 'week':
            start_date = (today - timedelta(days=7)).date()
            end_date = today.date()
        else:
            start_date = (today - timedelta(days=15)).date()
            end_date = today.date()

        start_month, end_month = get_months(start_date, end_date)
        if request['value'] == 'year':
            qry = '{call [Reporting].[sales_Analysis_sales_chart_lys] (?,?,?,?,?,?,?)}'
            params = (0, 0, decoded['comp_fk'], current_year, 0, 0, 0)
        elif request['value'] == 'month':
            qry = '{call [Reporting].[sales_Analysis_sales_chart_lys] (?,?,?,?,?,?,?)}'
            params = (0, end_month, decoded['comp_fk'], current_year, 0, 0, 0)
        else:
            qry = '{call [Reporting].[sales_Analysis_sales_chart_lys] (?,?,?,?,?,?,?)}'
            params = (1, 1, decoded['comp_fk'], current_year, 2, str(start_date), str(end_date))
        res, k = py_connection.call_prop1(qry, params)
        lst = []
        if res and len(res) > 0:
            for row in res:
                view_data = dict(zip(k, row))
                lst.append(view_data)
            return {"sales": lst, "bills": get_sales_bills(request, decoded),
                    "receipt_bill": get_receipt_bills(request, decoded), "last_updated_date": last_updated_date_for_sales(decoded)}
        else:
            return {"sales": lst, "bills": get_sales_bills(request, decoded),
                    "receipt_bill": get_receipt_bills(request, decoded), "last_updated_date": last_updated_date_for_sales(decoded)}
    except Exception as e:
        print(str(e))
        return {"sales": [], "bills": []}

# def get_month_name(month_number):
#     months = {
#         1: 'Jan',
#         2: 'Feb',
#         3: 'Mar',
#         4: 'Apr',
#         5: 'May',
#         6: 'Jun',
#         7: 'Jul',
#         8: 'Aug',
#         9: 'Sep',
#         10: 'Oct',
#         11: 'Nov',
#         12: 'Dec'
#     }
#     return months.get(month_number)


def get_sales_bills(request, decoded):
    try:
        current_year = dt.now().year
        # current_month = dt.now().month
        today = dt.today()
        if request['value'] == 'year':
            start_date = dt(current_year, 1, 1).date()
            end_date1 = dt(current_year + 1, 1, 1).date()
            end_date = end_date1 - timedelta(days=1)
        elif request['value'] == 'month':
            start_date = (today.replace(day=1)).date()
            end_date = today.date()
        elif request['value'] == 'week':
            start_date = (today - timedelta(days=7)).date()
            end_date = today.date()
        else:
            start_date = (today - timedelta(days=15)).date()
            end_date = today.date()
        qry = '{call Reporting.[sales_analysis_bills_lys] (?,?,?,?)}'
        res, k = py_connection.call_prop1(qry, (str(start_date), str(end_date), decoded['comp_fk'], ''))
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

def get_receipt_bills(request, decoded):
    try:
        current_year = dt.now().year
        # current_month = dt.now().month
        today = dt.today()
        if request['value'] == 'year':
            start_date = dt(current_year, 1, 1).date()
            end_date1 = dt(current_year + 1, 1, 1).date()
            end_date = end_date1 - timedelta(days=1)
        elif request['value'] == 'month':
            start_date = (today.replace(day=1)).date()
            end_date = today.date()
        elif request['value'] == 'week':
            start_date = (today - timedelta(days=7)).date()
            end_date = today.date()
        else:
            start_date = (today - timedelta(days=15)).date()
            end_date = today.date()
        qry = '{call Reporting.[receipt_bills_lys] (?,?,?,?)}'
        res, k = py_connection.call_prop1(qry, (str(start_date), str(end_date), decoded['comp_fk'], ''))
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


def get_sales_customer_pie_chart(request, decoded):
    try:
        print(request, "1111")
        current_year = dt.now().year
        # current_month = dt.now().month
        today = dt.today()
        if request['value'] == 'year':
            start_date = dt(current_year, 1, 1).date()
            end_date1 = dt(current_year + 1, 1, 1).date()
            end_date = end_date1 - timedelta(days=1)
        elif request['value'] == 'month':
            start_date = (today.replace(day=1)).date()
            end_date = today.date()
        elif request['value'] == 'week':
            start_date = (today - timedelta(days=7)).date()
            end_date = today.date()
        else:
            start_date = (today - timedelta(days=15)).date()
            end_date = today.date()

        qry = '{call [Reporting].[sales_analysis_customer_pie_chart_lys] (?,?,?,?)}'
        params = (request['type'], str(start_date), str(end_date), decoded['comp_fk'])
        res, k = py_connection.call_prop1(qry, params)
        lst = []
        if res and len(res) > 0:
            for row in res:
                view_data = dict(zip(k, row))
                lst.append(view_data)
            return {"customer": lst}
        else:
            return {"customer": lst}
    except Exception as e:
        print(str(e))
        return {"customer": []}


def get_sales_type_chart(request, decoded):
    try:
        print(request, "1111")
        current_year = dt.now().year
        # current_month = dt.now().month
        today = dt.today()
        if request['value'] == 'year':
            start_date = dt(current_year, 1, 1).date()
            end_date1 = dt(current_year + 1, 1, 1).date()
            end_date = end_date1 - timedelta(days=1)
        elif request['value'] == 'month':
            start_date = (today.replace(day=1)).date()
            end_date = today.date()
        elif request['value'] == 'week':
            start_date = (today - timedelta(days=7)).date()
            end_date = today.date()
        else:
            start_date = (today - timedelta(days=15)).date()
            end_date = today.date()

        qry = '{call [Reporting].[sales_analysis_type_chart_lys] (?,?,?)}'
        params = (str(start_date), str(end_date), decoded['comp_fk'])
        res, k = py_connection.call_prop1(qry, params)
        lst = []
        if res and len(res) > 0:
            for row in res:
                view_data = dict(zip(k, row))
                lst.append(view_data)
            print(lst,"1111")
            return {"sales_type": lst, "date": last_updated_date_for_sales(decoded)}
        else:
            return {"sales_type": lst, "date": last_updated_date_for_sales(decoded)}
    except Exception as e:
        print(str(e))
        return {"sales_type": []}


def last_updated_date_for_sales(decoded):
    qry = ("select  top 1 format(UpdatedDate, 'dd-MM-yyyy') as last_updated_date from RptSPG.salesreport where "
           "CompanyID =" + str(decoded['comp_fk']) + " order by UpdatedDate desc")
    res = py_connection.get_result(qry)
    return "As On " + str(res[0][0])