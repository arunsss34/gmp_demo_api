from db_connection import py_connection

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

def get_day():
    try:
        qry = "select day from Reporting.ageing_configuration where is_active = 1"
        res = py_connection.get_result(qry)
        return res
    except Exception as e:
        print(str(e))


def format_decimal(value):
    int_value = int(value[0])
    return int_value


def get_report_table(request, decoded):  # V1 -----> # All Agent Graph details starts here ---------------->
    try:
        config_details = get_table_config_details(request, decoded)
        if config_details and len(config_details) > 0:
            agent_info = get_agent_info(config_details[0][0], config_details[0][1])
            return agent_info
        else:
            return []
    except Exception as e:
        print(str(e))
        return []


def get_agent_info(report_table, report_config_fk):
    try:
        qry = "select all_agent_graph from Reporting." + str(report_table) + " where report_config_pk =" + str(report_config_fk)
        res = py_connection.get_result(qry)
        if res and len(res) > 0:
            return res[0][0]
        else:
            return []
    except Exception as e:
        print(str(e))
        return []


def get_agent_graph(request, decoded):
    try:
        agent_info = get_report_table(request, decoded)
        day = get_day()
        lst = []
        for row in day:
            date = row[0]
            if date.startswith('>'):
                start = int(date[1:])
                end = 0
            else:
                start, end = map(int, date.split('-'))
            graph_details = get_agent_graph_details(start, end, agent_info)
            x_axis = {row[0]: graph_details}
            lst.append(x_axis)
        return lst
    except Exception as e:
        print(str(e))



def get_agent_graph_details(start, end, procedure_name):
    try:
        qry = '{call Reporting.' + str(procedure_name) + ' (?,?)}'
        res = py_connection.call_prop1(qry, (start, end))
        if res and len(res) > 0:
            formatted_decimal = format_decimal(res[0][0])
            return formatted_decimal
        else:
            return 0
    except Exception as e:
        print(str(e))
        return 0   # All Agent Graph details ends here <----------------


def get_each_agent_graph(request, decoded):  # Each Agent Graph details starts here ------------>
    try:
        each_party_info = get_report_table1(request, decoded)
        day = get_day()
        lst = []
        for row in day:
            date = row[0]
            if date.startswith('>'):
                start = int(date[1:])
                end = 0
            else:
                start, end = map(int, date.split('-'))
            graph_details = get_each_agent_graph_details(start, end, each_party_info, request)
            x_axis = {row[0]: sum(graph_details)}
            lst.append(x_axis)
        return lst
    except Exception as e:
        print(str(e))

def get_report_table1(request, decoded):
    try:
        config_details = get_table_config_details(request, decoded)
        if config_details and len(config_details) > 0:
            each_agent_info = get_each_agent_info(config_details[0][0], config_details[0][1])
            return each_agent_info
        else:
            return []
    except Exception as e:
        print(str(e))
        return []

def get_each_agent_info(report_table, report_config_fk):
    try:
        qry = "select each_agent_graph from Reporting." + str(report_table) + " where report_config_pk =" + str(report_config_fk)
        res = py_connection.get_result(qry)
        if res and len(res) > 0:
            return res[0][0]
        else:
            return []
    except Exception as e:
        print(str(e))
        return []


def get_each_agent_graph_details(start, end, procedure_name, request):
    try:
        result = []
        for agent in request['agent_id']:
            agent_id = agent['id']
            company = agent['Company']
            qry = '{call Reporting.' + str(procedure_name) + ' (?,?,?,?)}'
            res = py_connection.call_prop1(qry, (agent_id, start, end, company))
            if res and len(res) > 0:
                formatted_decimal = format_decimal(res[0][0])
                result.append(formatted_decimal)
            else:
                result.append(0)
        return result
    except Exception as e:
        print(str(e))
        return 0    # Each Agent Graph details ends here <------------


def get_each_party_graph(request, decoded):  # Each Party Graph details Starts here ------------>
    try:
        each_party_info = get_report_table2(request, decoded)
        day = get_day()
        lst = []
        for row in day:
            date = row[0]
            if date.startswith('>'):
                start = int(date[1:])
                end = 0
            else:
                start, end = map(int, date.split('-'))
            graph_details = get_each_party_graph_details(start, end, each_party_info, request)
            x_axis = {row[0]: graph_details}
            lst.append(x_axis)
        return lst
    except Exception as e:
        print(str(e))


def get_report_table2(request, decoded):
    try:
        config_details = get_table_config_details(request, decoded)
        if config_details and len(config_details) > 0:
            each_agent_info = get_each_party_info(config_details[0][0], config_details[0][1])
            return each_agent_info
        else:
            return []
    except Exception as e:
        print(str(e))
        return []

def get_each_party_info(report_table, report_config_fk):
    try:
        qry = "select each_party_graph from Reporting." + str(report_table) + " where report_config_pk =" + str(report_config_fk)
        res = py_connection.get_result(qry)
        if res and len(res) > 0:
            return res[0][0]
        else:
            return []
    except Exception as e:
        print(str(e))
        return []

def get_each_party_graph_details(start, end, procedure_name, request):
    try:
        qry = '{call Reporting.' + str(procedure_name) + ' (?,?,?,?,?)}'
        res = py_connection.call_prop1(qry, (request['agent_id'], request['customer_id'], start, end, request['company']))
        if res and len(res) > 0:
            formatted_decimal = format_decimal(res[0][0])
            return formatted_decimal
        else:
            return 0
    except Exception as e:
        print(str(e))
        return 0  # Each Party Graph details ends here <------------