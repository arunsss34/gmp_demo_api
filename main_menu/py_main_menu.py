from db_connection import py_connection
#
#
# def main_menu(decoded):
#     # Getting the Sidebar items
#     try:
#         qry = ("select menupk,menu_description,icon from Reporting.main_menu where is_active = 1 and sublist_fk = 0 and "
#                "comp_fk =") + str(decoded['comp_fk']) + " and privilage LIKE " + "'%" + str(decoded['role_fk']) + "%'"
#         res, k = py_connection.get_result_col(qry)
#         lst = []
#         if res and len(res) > 0:
#             for row in res:
#                 view_data = dict(zip(k, row))
#                 lst.append(view_data)
#                 if len(get_main_menu_sublist(row[0], decoded)) > 0:
#                     view_data['sublist'] = get_main_menu_sublist(row[0], decoded)
#                 else:
#                     view_data['sublist'] = ''
#             return {"main_menu": lst}
#         else:
#             return {"main_menu": lst}
#     except Exception as e:
#         print(str(e))  # If no results, return an error message indicating incorrect username or password
#         return {"main_menu": []}
#
# def get_main_menu_sublist(main_menu_fk, decoded):
#     try:
#         qry = "select menupk,menu_description,icon from Reporting.main_menu where is_active = 1 and comp_fk =" + str(decoded['comp_fk']) + " and privilage LIKE " + "'%" + str(decoded['role_fk']) + "%' and sublist_fk =" + str(main_menu_fk) + " and sublist_fk <> 0 order by [order] asc"
#         res, k = py_connection.get_result_col(qry)
#         lst = []
#         if res and len(res) > 0:
#             for row in res:
#                 view_data = dict(zip(k, row))
#                 lst.append(view_data)
#                 if len(get_main_menu_sublist(row[0], decoded)) > 0:
#                     view_data['s_sublist'] = get_main_menu_sublist(row[0], decoded)
#                 else:
#                     view_data['s_sublist'] = ''
#             return lst
#         else:
#             return lst
#     except Exception as e:
#         print(str(e))  # If no results, return an error message indicating incorrect username or password
#         return []


def main_menu(decoded):
    try:
        qry = (
            "SELECT menupk, menu_description, icon, sublist_fk "
            "FROM Reporting.main_menu "
            "WHERE is_active = 1 AND comp_fk = {comp_fk} AND privilage LIKE '%{role_fk}%' "
            "ORDER BY [order] ASC"
        ).format(comp_fk=decoded['comp_fk'], role_fk=decoded['role_fk'])

        res, k = py_connection.get_result_col(qry)
        menu_dict = {}

        # Organize menus by sublist_fk
        for row in res:
            menu_item = dict(zip(k, row))
            menu_dict.setdefault(row[k.index('sublist_fk')], []).append(menu_item)

        # Recursive function to build sublist hierarchy
        def build_sublist(menu_fk):
            sublist = []
            if menu_fk in menu_dict:
                for item in menu_dict[menu_fk]:
                    item['sublist'] = build_sublist(item['menupk'])
                    sublist.append(item)
            return sublist

        # Build main menu
        menu = build_sublist(0)
        return {"main_menu": menu}

    except Exception as e:
        print(str(e))
        return {"main_menu": []}