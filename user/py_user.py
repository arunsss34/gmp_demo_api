from db_connection import py_connection
from datetime import datetime as dt


def get_role():  # V1 --> Role
    try:
        qry = "select role_pk,role from Reporting.role where is_active = 1 and role_pk <> 1"
        res, k = py_connection.get_result_col(qry)
        lst = []
        if res and len(res) > 0:
            for row in res:
                view_data = dict(zip(k, row))
                lst.append(view_data)
            return {"role": lst}
        else:
            return {"role": lst}
    except Exception as e:
        print(str(e))
        return {"role": []}


def get_user_list(decoded):
    try:
        qry = ("select user_pk,name,address,location,mobile,comp_fk,role_fk,role from Reporting.v_users "
               "where role_fk <> 1 and comp_fk=" + str(decoded['comp_fk']))
        res, k = py_connection.get_result_col(qry)
        lst = []
        if res and len(res) > 0:
            for row in res:
                view_data = dict(zip(k, row))
                lst.append(view_data)
            return {"user_list": lst}
        else:
            return {"user_list": lst}
    except Exception as e:
        print(str(e))
        return {"user_list": []}

def add_user(request, decoded):  # V1 ---> Add user based on company
    try:
        print(request,"111")
        qry = ("insert into Reporting.users(name,address,location,mobile,comp_fk,role_fk,is_active,created_date,"
               "created_by,updated_date,updated_by) values(?,?,?,?,?,?,?,?,?,?,?)")
        params = (request['name'], request['address'], request['location'], request['mobile'], decoded['comp_fk'],
                  request['role_fk'], 1, dt.now(), decoded['user_fk'], dt.now(), decoded['user_fk'])
        py_connection.put_result(qry, params)
        return {"message": "User detail added successfully", "rval": 1}
    except Exception as e:
        print(str(e))
        return {"message": "Adding of user details failed", "rval": 0}

def edit_user(request, decoded):
    try:
        print(request,"---")
        qry = "update Reporting.users set name=?,address=?,location=?,mobile=?,role_fk=?,updated_date=?,updated_by=? where user_pk =?"
        params = (request['name'], request['address'], request['location'], request['mobile'], request['role_fk'], dt.now(), decoded['user_fk'], request['user_pk'])
        py_connection.put_result(qry, params)
        return {"message": "User details edited successfully", "rval": 1}
    except Exception as e:
        print(str(e))
        return {"message": "Editing of user details failed", "rval": 0}

def delete_user(request, decoded):
    try:
        qry = "update Reporting.users set is_active = 0 where user_pk=? and comp_fk=?"
        py_connection.put_result(qry, (request['user_pk'], decoded['comp_fk']))
        return {"message": "User details deleted successfully", "rval": 1}
    except Exception as e:
        print(str(e))
        return {"message": "Deleting of user details failed", "rval": 0}

