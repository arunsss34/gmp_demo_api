from db_connection import py_connection
from collections import defaultdict

def get_report_table(request, decoded):  # V1 ----->Party wise Agent info Api starts here
    try:
        config_details = get_table_config_details(request, decoded)
        if config_details and len(config_details) > 0:
            agent_info, outstanding_total, value = get_agent_info(config_details[0][0], config_details[0][1], request)
            return {"agent_details": agent_info, "outstanding_total": outstanding_total, "value": value}
        else:
            return {"agent_details": [], "outstanding_total": '', "value": ''}
    except Exception as e:
        print(str(e))
        return {"agent_details": [], "outstanding_total": '', "value": ''}


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


def get_agent_info(report_table, report_config_fk, request):
    try:
        qry = "select agent_info,outstanding_total from Reporting." + str(
            report_table) + " where report_config_pk =" + str(report_config_fk)
        res = py_connection.get_result(qry)
        if res and len(res) > 0:
            agent_details = get_agent_details(res[0][0], request)
            outstanding_total, value = get_outstanding_total(res[0][1], request)
            return agent_details, outstanding_total, value
        else:
            return []
    except Exception as e:
        print(str(e))
        return []


def get_from_to_dt(day):
    if day.startswith('>'):
        start = int(day[1:])
        end = 0
    else:
        start, end = map(int, day.split('-'))
    return start, end


def get_agent_details(procedure_name, request):
    try:
        if request.get("day") == 'All':
            start_dt = 0
            end_dt = 0
            is_all = 1
        else:
            start_dt, end_dt = get_from_to_dt(request.get("day"))
            is_all = 0
        qry = '{call Reporting.' + str(procedure_name) + '(?,?,?)}'
        res, k = py_connection.call_prop_col(qry, (start_dt, end_dt, is_all))
        lst = []
        if res and len(res) > 0:
            for row in res:
                view_data = dict(zip(k, row))
                lst.append(view_data)
            agent = agent_with_company_details(lst)
            return agent
        else:
            return lst
    except Exception as e:
        print(str(e))
        return [], 0


def agent_with_company_details(agent_data):
    try:
        agent_dict = defaultdict(lambda: {'data': [], 'TotalBalanceAmount': 0})

        for row in agent_data:
            agent_key = (row['CustomerName'], row['GSTNo'])
            agent_dict[agent_key]['data'].append({
                'id': row['CustomerID'],
                'Company': row['Company']
            })
            # Summing the TotalBalanceAmount (assumed Decimal has been removed)
            agent_dict[agent_key]['TotalBalanceAmount'] += row['TotalBalanceAmount']

        # Convert to the desired output format
        result = [
            {'CustomerName': key[0], 'GSTNo': key[1], 'data': value['data'],
             'BalanceAmount': value['TotalBalanceAmount']}
            for key, value in agent_dict.items()
        ]
        return result
    except Exception as e:
        print(str(e))



def get_outstanding_total(procedure_name, request):
    try:
        if request.get("day") == 'All':
            start_dt = 0
            end_dt = 0
            is_all = 1
        else:
            start_dt, end_dt = get_from_to_dt(request.get("day"))
            is_all = 0
        qry = '{call Reporting.' + str(procedure_name) + '(?,?,?)}'
        res, k = py_connection.call_prop_col(qry, (start_dt, end_dt, is_all))
        if res and len(res) > 0:
            value = int(res[0][0])
            date = res[0][1]
            return " as of " + str(date) + " | " + str(request.get("day")), str(value)
        else:
            return '', ''
    except Exception as e:
        print(str(e))
        return '', ''   # Party wise Agent info Api ends here


def get_party_info_by_agent_id(request, decoded):  # Party info Api starts here ------------->
    try:
        config_details = get_table_config_details(request, decoded)
        if config_details and len(config_details) > 0:
            party_info, customer_name, outstanding_sum, last_updated_date = get_party_info(config_details[0][0], config_details[0][1], request)
            return {"agent_info": party_info, "customer_name": customer_name, "outstanding_sum": " as of " + str(last_updated_date) + " | " + str(request.get("day")), "value": str(outstanding_sum)}
        else:
            return {"agent_info": [], "customer_name": [], "outstanding_sum": '', "value": ''}
    except Exception as e:
        print(str(e))
        return {"agent_info": [], "customer_name": [], "outstanding_sum": '', "value": ''}


def get_party_info(report_table, report_config_fk, request):
    try:
        qry = "select party_info from Reporting." + str(report_table) + " where report_config_pk =" + str(report_config_fk)
        res = py_connection.get_result(qry)
        if res and len(res) > 0:
            agent_details, outstanding_sum = get_party_basic_details(res[0][0], request)
            return agent_details, agent_details[0]['CustomerName'], outstanding_sum, agent_details[0]['last_updated_date']
        else:
            return [], '', 0, ''
    except Exception as e:
        print(str(e))
        return [], '', 0, ''

def get_party_basic_details(procedure_name, request):
    try:
        if request.get("day") == 'All':
            start_dt = 0
            end_dt = 0
            is_all = 1
        else:
            start_dt, end_dt = get_from_to_dt(request.get("day"))
            is_all = 0

        total_outstanding_sum = 0
        result_data = []

        for agent in request['agent_id']:
            agent_id = agent['id']
            company = agent['Company']
            qry = '{call Reporting.' + str(procedure_name) + ' (?,?,?,?,?)}'
            res, k = py_connection.call_prop1(qry, (agent_id, start_dt, end_dt, is_all, company))
            lst = []
            outstanding_sum = 0
            customer_name = None
            last_updated_date = ''
            if res and len(res) > 0:
                for row in res:
                    view_data = dict(zip(k, row))
                    outstanding_sum += row[3]  # Assuming row[3] is 'TotalBalanceAmount' without decimal
                    customer_name = view_data['CustomerName']
                    last_updated_date = view_data['last_updated_date']
                    lst.append(view_data)

                result_data.append({
                    "CustomerName": customer_name,
                    "last_updated_date": last_updated_date,
                    company: lst,
                    "TotalBalanceAmount": outstanding_sum
                })
                total_outstanding_sum += outstanding_sum
        return result_data, total_outstanding_sum
    except Exception as e:
        print(str(e))
        return [], 0



# def get_party_basic_details(procedure_name, request):
#     try:
#         if request.get("day") == 'All':
#             start_dt = 0
#             end_dt = 0
#             is_all = 1
#         else:
#             start_dt, end_dt = get_from_to_dt(request.get("day"))
#             is_all = 0
#
#         total_outstanding_sum = 0
#         result_data = []
#         ids = [agent['id'] for agent in request['agent_id']]
#         print(ids, '----')
#         companies = [agent['Company'] for agent in request['agent_id']]
#         print(companies, '999')
#
#         if sorted(companies) == sorted(['Lucky Yarn Tex India Private Limited', 'Lucky Weaves India Private Limited']):
#             qry = '{call Reporting.' + str(procedure_name) + ' (?,?,?,?,?)}'
#             res, k = py_connection.call_prop1(qry, (start_dt, end_dt, is_all, ids[0], ids[1]))
#
#
#         elif sorted(companies) == sorted(['Lucky Yarn Tex India Private Limited']):
#             qry = '{call Reporting.' + str(procedure_name) + ' (?,?,?,?,?)}'
#             res, k = py_connection.call_prop1(qry, (start_dt, end_dt, is_all, ids[0], 0))
#
#         else:
#             qry = '{call Reporting.' + str(procedure_name) + ' (?,?,?,?,?)}'
#             res, k = py_connection.call_prop1(qry, (start_dt, end_dt, is_all, 0, ids[0]))
#
#         lst = []
#         outstanding_sum = 0
#         agent_name = None
#         last_updated_date = ''
#         if res and len(res) > 0:
#             for row in res:
#                 view_data = dict(zip(k, row))
#                 outstanding_sum += view_data['TotalBalanceAmount']  # Assuming row[4] is 'TotalBalanceAmount' without decimal
#                 agent_name = view_data['AgentName']
#                 last_updated_date = view_data['last_updated_date']
#                 lst.append(view_data)
#
#             result_data.append({
#                 "AgentName": agent_name,
#                 "last_updated_date": last_updated_date,
#                 "TotalBalanceAmount": outstanding_sum,
#                 "details": party_with_company_details(lst)
#             })
#             total_outstanding_sum += outstanding_sum
#         return result_data, total_outstanding_sum
#     except Exception as e:
#         print(str(e))
#         return [], 0
#
# def party_with_company_details(agent_data):
#     try:
#         agent_dict = defaultdict(lambda: {'agent_id': [], 'customer_id': [], 'TotalBalanceAmount': 0})
#
#         for row in agent_data:
#             agent_key = (row['customerName'], row['GstNo'])
#             agent_dict[agent_key]['agent_id'].append({
#                 'id': row['AgentID'],
#                 'Company': row['Company']
#             })
#             agent_dict[agent_key]['customer_id'].append({
#                 'id': row['customerId'],
#                 'Company': row['Company']
#             })
#             # Summing the TotalBalanceAmount (assumed Decimal has been removed)
#             agent_dict[agent_key]['TotalBalanceAmount'] += row['TotalBalanceAmount']
#
#         # Convert to the desired output format
#         result = [
#             {'customerName': key[0], 'GstNo': key[1], 'agent_id': value['agent_id'],
#              'customer_id': value['customer_id'],'BalanceAmount': value['TotalBalanceAmount']}
#             for key, value in agent_dict.items()
#         ]
#         return result
#     except Exception as e:
#         print(str(e))


def get_party_details_by_agent_id(request, decoded):  # Party Details Api starts Here
    try:
        config_details = get_table_config_details(request, decoded)
        if config_details and len(config_details) > 0:
            party_info, tds_amount, party_total, party_name, agent_name, outstanding_sum = get_party_info1(
                config_details[0][0], config_details[0][1], request)
            return {"party_details": party_info, "tds_amount": tds_amount, "party_total": party_total,
                    "party_name": party_name, "agent_name": agent_name,
                    "outstanding_sum": '₹' + str(outstanding_sum) + " | " + str(request.get("day"))}
        else:
            return {"party_details": [], "tds_amount": 0, "party_total": 0, "party_name": '',
                    "agent_name": '', "outstanding_sum": ''}
    except Exception as e:
        print(str(e))
        return {"party_details": [], "tds_amount": 0, "party_total": 0, "party_name": '', "agent_name": '',
                "outstanding_sum": ''}


def get_party_info1(report_table, report_config_fk, request):
    try:
        qry = "select party_details from Reporting." + str(report_table) + " where report_config_pk =" + str(report_config_fk)
        res = py_connection.get_result(qry)
        if res and len(res) > 0:
            agent_details, outstanding_sum = get_party_full_details(res[0][0], request)
            if agent_details and len(agent_details) > 0:
                tds_amount = agent_details[0]['TDSBalanceAmount']
                party_total = agent_details[0]['party_total']
                party_name = agent_details[0]['CustomerName']
                agent_name = agent_details[0]['AgentName']
                return agent_details, tds_amount, party_total, party_name, agent_name, outstanding_sum
            else:
                return [], 0, 0, '', 0
        else:
            return []
    except Exception as e:
        print(str(e))
        return [], 0, 0, '', 0


def get_party_full_details(procedure_name, request):
    try:
        if request.get("day") == 'All':
            start_dt = 0
            end_dt = 0
            is_all = 1
        else:
            start_dt, end_dt = get_from_to_dt(request.get("day"))
            is_all = 0
        qry = '{call Reporting.' + str(procedure_name) + ' (?,?,?,?,?,?)}'
        res, k = py_connection.call_prop1(qry, (request['agent_id'],
                                                request['customer_id'], start_dt, end_dt, is_all,
                                                request['company']))
        lst = []
        outstanding_sum = 0
        if res and len(res) > 0:
            for row in res:
                view_data = dict(zip(k, row))
                lst.append(view_data)
                outstanding_sum += row[8]
            return lst, outstanding_sum
        else:
            return lst, outstanding_sum
    except Exception as e:
        print(str(e))
        return [], 0


# def get_party_full_details(procedure_name, request):
#     try:
#         if request.get("day") == 'All':
#             start_dt = 0
#             end_dt = 0
#             is_all = 1
#         else:
#             start_dt, end_dt = get_from_to_dt(request.get("day"))
#             is_all = 0
#
#         total_sum = 0
#         result_data = []
#
#         print(request)
#         for agent in request['agent_id']:
#             agent_id = agent['id']
#             company = agent['Company']
#             for customer in request['customer_id']:
#                 if customer['Company'] == company:
#                     customer_id = customer['id']
#
#                     qry = '{call Reporting.' + str(procedure_name) + ' (?,?,?,?,?,?)}'
#                     res, k = py_connection.call_prop1(qry,
#                         (agent_id, customer_id, start_dt, end_dt, is_all, company)
#                     )
#                     lst = []
#                     outstanding_sum = 0
#                     if res and len(res) > 0:
#                         for row in res:
#                             view_data = dict(zip(k, row))
#                             lst.append(view_data)
#                             outstanding_sum += row[8]
#                         result_data.append({
#                             "AgentName": lst[0]['AgentName'],
#                             "CustomerName": lst[0]['CustomerName'],
#                             "TDSBalanceAmount": lst[0]['TDSBalanceAmount'],
#                             "party_total": lst[0]['party_total'],
#                             "outstanding_sum": '₹' +
#                                                str(lst[0]['party_total']) + " | " + str(request.get("day")),
#                             company: lst
#                         })
#                         total_sum += outstanding_sum
#         return result_data, total_sum
#     except Exception as e:
#         print(str(e))
#         return [], 0