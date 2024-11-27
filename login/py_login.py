import base64
from db_connection import py_connection
from Crypto.Cipher import DES3
from auth.py_jwt import signJWT

key = b'Binary--Solution'

def login(request):
    try:
        # Extracting username and password from the request
        username = request.get("username")
        password = request.get("password")

        # Constructing the SQL query to check the username and password in the database
        qry = ("select username,user_fk,comp_fk,role_fk,name from Reporting.v_login"
               " where is_active = 1 and username = '{0}' and password = '{1}'".format(username, password))

        # Execute the query and get the result along with column names
        result, k = py_connection.get_result_col(qry)
        lst = []

        # If there are results, process them
        if result and len(result) > 0:
            for row in result:
                # Create a dictionary for each row with column names as keys
                view_data = dict(zip(k, row))
                lst.append(view_data)
            # Generate a JWT token with the username, employee ID, and role ID
            token = signJWT(result[0][0], result[0][1], result[0][2], result[0][3], result[0][4])
            # Return a success message with the token
            return {"message": "Login successfully", "rval": 1, "token": token}
        else:
            # If no results, return an error message indicating incorrect username or password
            return {"message": "Username or password is incorrect", "rval": 0, "token": ""}

    except Exception as e:
        # If there's an exception, print the error and return a generic error message
        print(str(e))
        return {"message": "Something went wrong", "rval": 0}


def pad(data):
    # Calculate the padding length needed to make the data a multiple of 8 bytes
    padding_length =8 -len(data) % 8

    # Append the padding bytes (each with the value of the padding length) to the data
    return data + bytes([padding_length] * padding_length)


def unpad(data):
    # The padding length is stored in the last byte of the data
    padding_length = data[-1]

    # Remove the padding bytes from the data
    return data[:-padding_length]


def encrypt_password(password,key):
    # Create a DES3 cipher object in ECB mode with the provided key
    cipher = DES3.new(key, DES3.MODE_ECB)

    # Pad the password to ensure its length is a multiple of 8 bytes
    padded_password = pad(password.encode())

    # Encrypt the padded password
    encrypt_password = cipher.encrypt(padded_password)

    # Encode the encrypted password in base64 to make it safe for storage/transmission
    return base64.b64encode(encrypt_password).decode()

def decrypt_password(encrypted_password, key):
    # Create a DES3 cipher object in ECB mode with the provided key
    cipher = DES3.new(key, DES3.MODE_ECB)

    # Decode the base64-encoded encrypted password
    encrypted_password = base64.b64decode(encrypted_password)

    # Decrypt the encrypted password
    decrypted_password = cipher.decrypt(encrypted_password)

    # Remove the padding from the decrypted password and return it as a string
    return unpad(decrypted_password).decode()


def new_password(request, decoded):
    try:
        # Retrieve the old and new passwords from the request
        old_password = str(request.get("oldpwd"))
        new_password = str(request.get("newpwd"))

        # Get the employee foreign key (emp_fk) from the decoded data
        emp_fk = decoded.get("emp_fk")

        # Query to select the current password for the given employee
        qry1 = "select password from tmsranga.Web_task_logins where emp_fk =" + str(emp_fk)

        # Execute the query and store the result
        res1 = py_connection.get_result(qry1)
        if res1 and len(res1) > 0:
            # Function to Check if the old password provided by the user matches the current password
            check_and_update_password(res1, old_password, new_password, emp_fk)
        else:
            return {"message": "There is no password registered for the Employee id", "rval": 0}
    except Exception as e:
        # If there's an exception, print the error and return a generic error message
        print(str(e))
        return {"message": "password updation failed", "rval": 0}

def check_and_update_password(res1, old_password,new_password, emp_fk):
    if str(res1[0][0]) == old_password:
        # If the passwords match, update the password with the new password
        qry = "update tmsranga.Web_task_logins set password = ? where emp_fk =" + str(emp_fk)
        py_connection.put_result(qry, new_password)
        return {"message": "password has been updated", "rval": 1}
    else:
        # If the old password does not match, return an error message
        return {"message": "old password is incorrect", "rval": 0}