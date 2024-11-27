from db_connection import py_connection
from datetime import datetime as dt, timedelta

# Lucky Yarns Stock Report
def get_stock_report_table(request, decoded):  # V1 -----> Agent info Api starts here
    try:
        config_details = get_table_config_details(request, decoded)
        if config_details and len(config_details) > 0:
            details, rpt_name, file_name = get_details(config_details[0][0], config_details[0][1], request)
            if request['type'] == 2:
                return {"details": details[0], "fields": details[1], "total":  details[2], "rpt_name": rpt_name, "file_name": file_name}
            elif request['type'] == 3:
                return {"details": details[0], "fields": details[1], "total": details[2], "rpt_name": rpt_name, "file_name": file_name}
            else:
                return {"details": details[0], "fields": details[1], "rpt_name": rpt_name, "file_name": file_name}
        else:
            return {"details": [], "rpt_name": [], "file_name": []}
    except Exception as e:
        print(str(e))
        return {"details": [], "rpt_name": [], "file_name": []}

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

def get_details(report_table, report_config_fk, request):
    try:
        qry = ("select item, item_rpt_name, item_file_name, category, category_rpt_name, category_file_name, "
               " frame, frame_rpt_name, frame_file_name from Reporting.") + str(report_table) + " where report_config_pk =" + str(report_config_fk)
        res = py_connection.get_result(qry)
        if res and len(res) > 0:
            if request['type'] == 1:
                item_details = get_item_details(res[0][0]), res[0][1], res[0][2]
            elif request['type'] == 2:
                item_details = get_category_details(res[0][3]), res[0][4], res[0][5]
            else:
                item_details = get_frame_details(res[0][6], request), res[0][7], res[0][8]
            return item_details
        else:
            return []
    except Exception as e:
        print(str(e))
        return []


def get_category_details(procedure_name):
    try:
        qry = '{call Reporting.' + str(procedure_name) + '}'
        res, k = py_connection.call_prop_col_without_param(qry)
        lst = []
        total_bags = 0.0
        total_pallet = 0.0
        total_cones = 0.0
        total_weight = 0.0

        if res and len(res) > 0:
            for row in res:
                view_data = dict(zip(k, row))
                lst.append(view_data)

                # Accumulate totals
                total_bags += int(row[k.index('Bags')])
                total_pallet += int(row[k.index('Pallet')])
                total_cones += int(row[k.index('Cones')])
                total_weight += float(row[k.index('TotalWeight')])

            # Create the total dictionary after the loop
            total = {
                'Type': 'Total',
                'Bags': total_bags,
                'Pallet': total_pallet,
                'Cones': total_cones,
                'TotalWeight': round(total_weight, 3)
            }


            return lst, k, total
        else:
            return lst, k, []
    except Exception as e:
        print(str(e))
        return [], [], []


def get_item_details(procedure_name):
    try:
        qry = '{call Reporting.' + str(procedure_name) + '}'
        res, k = py_connection.call_prop_col_without_param(qry)
        result = {}
        if res and len(res) > 0:
            for row in res:
                row_dict = {k[i]: row[i] for i in range(len(k))}
                location = row_dict.get('Location')
                rg_type = row_dict.get('Type')

                if location not in result:
                    result[location] = {}
                if rg_type not in result[location]:
                    result[location][rg_type] = []

                item_details = {key: value for key, value in row_dict.items() if key not in ['Location', 'Type']}
                result[location][rg_type].append(item_details)
        k = [item for item in k if item not in ['Location', 'Type']]
        return result, k
    except Exception as e:
        print(str(e))
        return []


def get_frame_details(procedure_name, request):
    try:
        date_str = request['date']
        # date_str = '2024-09-01'
        date_obj = dt.strptime(date_str, '%Y-%m-%d')
        previous_date_str = (date_obj - timedelta(days=1)).strftime('%Y-%m-%d')

        qry = '{call Reporting.' + str(procedure_name) + '(?)}'
        res, k = py_connection.call_prop1(qry, (date_str,))
        result = {}
        total = {}

        if res and len(res) > 0:
            last_values = {}  # Store the last row's values for each Type

            for row in res:
                row_dict = {k[i]: row[i] for i in range(len(k))}
                Type = row_dict.get('Type')

                if Type not in result:
                    result[Type] = []

                # Create item details excluding the 'Type'
                item_details = {key: value for key, value in row_dict.items() if key not in ['DeliveryBagsPerPallet', 'Type',
                                "Subtotal Bags / Pallet Stock", "Subtotal Loose Cones", "Subtotal R/F",
                                "Subtotal Delivery Bags / Pallet", "Subtotal PendingBags", "Total Bags / Pallet Stock",
                                "Total Loose Cones", "Total R/F", "Total DeliveryBagsPerPallet", "Total PendingBags"]}

                # Add the new key 'Delivery on <previous_date_str>'
                item_details['Delivery on ' + previous_date_str] = row_dict.get('DeliveryBagsPerPallet')

                result[Type].append(item_details)

                # Store the last row's values for each Type
                last_values[Type] = {
                    k[3]: row_dict.get("Subtotal Bags / Pallet Stock", 0),
                    k[4]: row_dict.get("Subtotal Loose Cones", 0),
                    k[5]: row_dict.get("Subtotal R/F", 0),
                    'Delivery on ' + previous_date_str: row_dict.get("Subtotal Delivery Bags / Pallet", 0),
                    k[7]: row_dict.get("Subtotal PendingBags", 0)
                }

                total = {
                    k[1]: '',
                    k[2]: 'Total',
                    k[3]: row_dict.get("Total Bags / Pallet Stock", 0),
                    k[4]: row_dict.get("Total Loose Cones", 0),
                    k[5]: row_dict.get("Total R/F", 0),
                    'Delivery on ' + previous_date_str: row_dict.get("Total DeliveryBagsPerPallet", 0),
                    k[7]: row_dict.get("Total PendingBags", 0)
                }

            # Now append the subtotal for each Type using the last row's values
            for Type in last_values:
                subtotal = {
                    k[1]: '',
                    k[2]: 'Subtotal',
                    k[3]: last_values[Type][k[3]],
                    k[4]: last_values[Type][k[4]],
                    k[5]: last_values[Type][k[5]],
                    'Delivery on ' + previous_date_str: last_values[Type]['Delivery on ' + previous_date_str],
                    k[7]: last_values[Type][k[7]]
                }
                result[Type].append(subtotal)
                total.update(total)

            # Append the new key to `k` after processing all rows
            # if 'Delivery on ' + previous_date_str not in k:
            #     k.append('Delivery on ' + previous_date_str)
            # Append the new key before k[7]
            new_key = 'Delivery on ' + previous_date_str
            if new_key not in k:
                k.insert(7, new_key)

            k = [item for item in k if item not in ['DeliveryBagsPerPallet', 'Type',
                                "Subtotal Bags / Pallet Stock", "Subtotal Loose Cones", "Subtotal R/F",
                                "Subtotal Delivery Bags / Pallet", "Subtotal PendingBags", "Total Bags / Pallet Stock",
                                "Total Loose Cones", "Total R/F", "Total DeliveryBagsPerPallet", "Total PendingBags"]]

            return result, k, total
        else:
            return '', '', ''
    except Exception as e:
        print(str(e))
        return '', '', ''

def last_updated_date_for_frame_wise_stock(decoded):
    qry = ("select  top 1 format(UpdatedDate, 'dd-MM-yyyy') as last_updated_date from RptSPG.YarnFrameWiseStock where "
           "CompanyID =" + str(decoded['comp_fk']) + " order by UpdatedDate desc")
    res = py_connection.get_result(qry)
    return str(res[0][0])

# Lucky Weaves Stock Report

def get_stock_report_table_lw(request, decoded):  # V1 -----> Agent info Api starts here
    try:
        config_details = get_table_config_details_lw(request, decoded)
        if config_details and len(config_details) > 0:
            details, rpt_name, file_name = get_details_lw(config_details[0][0], config_details[0][1], request)
            if request['type'] == 2:
                return {"details": details[0], "fields": details[1], "total":  details[2], "rpt_name": rpt_name, "file_name": file_name}
            elif request['type'] == 3:
                return {"details": details[0], "fields": details[1], "total": details[2], "rpt_name": rpt_name, "file_name": file_name}
            else:
                return {"details": details[0], "fields": details[1], "rpt_name": rpt_name, "file_name": file_name}
        else:
            return {"details": [], "rpt_name": [], "file_name": []}
    except Exception as e:
        print(str(e))
        return {"details": [], "rpt_name": [], "file_name": []}

def get_table_config_details_lw(request, decoded):
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

def get_details_lw(report_table, report_config_fk, request):
    try:
        qry = ("select item, item_rpt_name, item_file_name, category, category_rpt_name, category_file_name, "
               " frame, frame_rpt_name, frame_file_name from Reporting.") + str(report_table) + " where report_config_pk =" + str(report_config_fk)
        res = py_connection.get_result(qry)
        if res and len(res) > 0:
            if request['type'] == 1:
                item_details = get_item_details_lw(res[0][0]), res[0][1], res[0][2]
            elif request['type'] == 2:
                item_details = get_category_details_lw(res[0][3]), res[0][4], res[0][5]
            else:
                item_details = get_frame_details_lw(res[0][6], request), res[0][7], res[0][8]
            return item_details
        else:
            return []
    except Exception as e:
        print(str(e))
        return []

def get_category_details_lw(procedure_name):
    try:
        qry = '{call Reporting.' + str(procedure_name) + '}'
        res, k = py_connection.call_prop_col_without_param(qry)
        lst = []
        total_bags = 0.0
        total_pallet = 0.0
        total_cones = 0.0
        total_weight = 0.0

        if res and len(res) > 0:
            for row in res:
                view_data = dict(zip(k, row))
                lst.append(view_data)

                # Accumulate totals
                total_bags += int(row[k.index('Bags')])
                total_pallet += int(row[k.index('Pallet')])
                total_cones += int(row[k.index('Cones')])
                total_weight += float(row[k.index('TotalWeight')])

            # Create the total dictionary after the loop
            total = {
                'Type': 'Total',
                'Bags': total_bags,
                'Pallet': total_pallet,
                'Cones': total_cones,
                'TotalWeight': round(total_weight, 3)
            }


            return lst, k, total
        else:
            return lst, k, []
    except Exception as e:
        print(str(e))
        return [], [], []


def get_item_details_lw(procedure_name):
    try:
        qry = '{call Reporting.' + str(procedure_name) + '}'
        res, k = py_connection.call_prop_col_without_param(qry)
        result = {}
        if res and len(res) > 0:
            for row in res:
                row_dict = {k[i]: row[i] for i in range(len(k))}
                location = row_dict.get('Location')
                rg_type = row_dict.get('Type')

                if location not in result:
                    result[location] = {}
                if rg_type not in result[location]:
                    result[location][rg_type] = []

                item_details = {key: value for key, value in row_dict.items() if key not in ['Location', 'Type']}
                result[location][rg_type].append(item_details)
        k = [item for item in k if item not in ['Location', 'Type']]
        return result, k
    except Exception as e:
        print(str(e))
        return []


def get_frame_details_lw(procedure_name, request):
    try:
        date_str = request['date']
        # date_str = '2024-09-01'
        date_obj = dt.strptime(date_str, '%Y-%m-%d')
        previous_date_str = (date_obj - timedelta(days=1)).strftime('%Y-%m-%d')

        qry = '{call Reporting.' + str(procedure_name) + '(?)}'
        res, k = py_connection.call_prop1(qry, (date_str,))
        result = {}
        total = {}

        if res and len(res) > 0:
            last_values = {}  # Store the last row's values for each Type

            for row in res:
                row_dict = {k[i]: row[i] for i in range(len(k))}
                Type = row_dict.get('Type')

                if Type not in result:
                    result[Type] = []

                # Create item details excluding the 'Type'
                item_details = {key: value for key, value in row_dict.items() if key not in ['DeliveryBagsPerPallet', 'Type',
                                "Subtotal Bags / Pallet Stock", "Subtotal Loose Cones", "Subtotal R/F",
                                "Subtotal Delivery Bags / Pallet", "Subtotal PendingBags", "Total Bags / Pallet Stock",
                                "Total Loose Cones", "Total R/F", "Total DeliveryBagsPerPallet", "Total PendingBags"]}

                # Add the new key 'Delivery on <previous_date_str>'
                item_details['Delivery on ' + previous_date_str] = row_dict.get('DeliveryBagsPerPallet')

                result[Type].append(item_details)

                # Store the last row's values for each Type
                last_values[Type] = {
                    k[3]: row_dict.get("Subtotal Bags / Pallet Stock", 0),
                    k[4]: row_dict.get("Subtotal Loose Cones", 0),
                    k[5]: row_dict.get("Subtotal R/F", 0),
                    'Delivery on ' + previous_date_str: row_dict.get("Subtotal Delivery Bags / Pallet", 0),
                    k[7]: row_dict.get("Subtotal PendingBags", 0)
                }

                total = {
                    k[1]: '',
                    k[2]: 'Total',
                    k[3]: row_dict.get("Total Bags / Pallet Stock", 0),
                    k[4]: row_dict.get("Total Loose Cones", 0),
                    k[5]: row_dict.get("Total R/F", 0),
                    'Delivery on ' + previous_date_str: row_dict.get("Total DeliveryBagsPerPallet", 0),
                    k[7]: row_dict.get("Total PendingBags", 0)
                }

            # Now append the subtotal for each Type using the last row's values
            for Type in last_values:
                subtotal = {
                    k[1]: '',
                    k[2]: 'Subtotal',
                    k[3]: last_values[Type][k[3]],
                    k[4]: last_values[Type][k[4]],
                    k[5]: last_values[Type][k[5]],
                    'Delivery on ' + previous_date_str: last_values[Type]['Delivery on ' + previous_date_str],
                    k[7]: last_values[Type][k[7]]
                }
                result[Type].append(subtotal)
                total.update(total)

            # Append the new key to `k` after processing all rows
            # if 'Delivery on ' + previous_date_str not in k:
            #     k.append('Delivery on ' + previous_date_str)
            # Append the new key before k[7]
            new_key = 'Delivery on ' + previous_date_str
            if new_key not in k:
                k.insert(7, new_key)

            k = [item for item in k if item not in ['DeliveryBagsPerPallet', 'Type',
                                "Subtotal Bags / Pallet Stock", "Subtotal Loose Cones", "Subtotal R/F",
                                "Subtotal Delivery Bags / Pallet", "Subtotal PendingBags", "Total Bags / Pallet Stock",
                                "Total Loose Cones", "Total R/F", "Total DeliveryBagsPerPallet", "Total PendingBags"]]

            return result, k, total
        else:
            return '', '', ''
    except Exception as e:
        print(str(e))
        return '', '', ''

def last_updated_date_for_frame_wise_stock_lw():
    qry = ("select  top 1 format(UpdatedDate, 'dd-MM-yyyy') as last_updated_date from RptSPG.YarnFrameWiseStock where "
           "CompanyID = 2 order by UpdatedDate desc")
    res = py_connection.get_result(qry)
    return str(res[0][0])