from gevent import monkey
monkey.patch_all()
import gevent, os, time, sys, logging, pymysql, paramiko, json, yaml
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import as_completed
#=========================================================log===========================================================
def log():
    log_path = "./log/"
    os.makedirs(log_path, exist_ok=True)
    logger = logging.getLogger('admin')
    logger.setLevel(logging.INFO)

    consoleHandler = logging.StreamHandler()
    logger.setLevel(logging.INFO)
    fileHandler = logging.FileHandler(filename='./log/admin.log',mode=log_writing_mode)
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter('[%(asctime)s]-[%(levelname)-8s]-[%(lineno)s]:%(message)s')

    consoleHandler.setFormatter(formatter)
    fileHandler.setFormatter(formatter)

    logger.addHandler(consoleHandler)
    logger.addHandler(fileHandler)

    return logger
#================================================get host lsit for file================================================
def read_file():
    file_path = hosts_path
    logger.info(f"\033[33mReading file from {file_path}...\033[0m")
    with open(file_path, 'r') as file:
        ip_list = file.readlines()
        ip_list = [s.replace('\n', '') for s in ip_list]
        logger.info(f'\033[33mget {len(ip_list)} IP address\033[0m')
        return ip_list
#=================================================get host list for mysql===============================================
def mysql_connect():
    global ip_list
    ip_list = []
    db = pymysql.connect(host=db_host,
                         port=db_port,
                         user=db_user,
                         password=db_password,
                         database=db_database)

    cursor = db.cursor()
    logger.info('\033[33mDatabase connected\033[0m')


    cursor.execute(sql)

    data = cursor.fetchall()

    logger.info('\033[33mgetting IP address from database...\033[0m')

    for i in data:
        ip_list.append(i[0])

    logger.info(f'\033[33mget {len(ip_list)} IP address\033[0m')
    db.close()
    logger.info('\033[33mDatabase disconnected\033[0m')
    return ip_list
#======================================================ssh connect======================================================
def ssh_connect(ip):
    #logger.info(f'ssh connect to {ip}')
    try:
        if ssh_key_path == "":
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            ssh_client.connect(
                hostname=ip,
                port=ssh_port,
                username=ssh_username,
                timeout=10,
                password=ssh_password)
        else:
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            private = paramiko.RSAKey.from_private_key_file(ssh_key_path)
            ssh_client.connect(
                hostname=ip,
                port=ssh_port,
                username=ssh_username,
                timeout=10,
                pkey=private)

    except TimeoutError as e:
        logger.error(f"\033[31m{ip} 连接超时 {e}\033[0m")
        ssh_client.close()
        return "Failed"

    except paramiko.ssh_exception.AuthenticationException as e:
        logger.error(f"\033[31m{ip} 认证失败{e}\033[0m")
        ssh_client.close()
        return "Failed"
    except paramiko.ssh_exception.SSHException as e:
        logger.error(f"\033[31m{ip} 无法连接 {e}\033[0m")
        ssh_client.close()
        return "Failed"
    except Exception as e:
        logger.error(f"\033[31m{ip} {e}\033[0m")
        ssh_client.close()
        return "Failed"

    return ssh_client
#======================================================ssh server=======================================================
def ssh_server(ip):
    output = error = ""
    ssh_client = ssh_connect(ip)
    if ssh_client == "Failed" :
        return ip, "Failed"

    for command in commands:
        stdin, stdout, stderr = ssh_client.exec_command(command)
        # output = stdout.read().decode()
        # error = stderr.read().decode()
        if output is not None:
            output += stdout.read().decode() + "\n"
        else:
            error += stderr.read().decode() + "\n"

    ssh_result = output+error
    ssh_client.close()
    logger.info(f"\033[32m{ip} is Complete\033[0m")
    return ip, ssh_result
    #.replace("\n", "")
#======================================================sftp server======================================================
def sftp_server(ip):
    ssh_client = ssh_connect(ip)
    if ssh_client == False :
        return ip, "Failed"

    sftp = ssh_client.open_sftp()
    try:
        if sftp_mode == "get":
            sftp.get(remotepath,localpath)
            logger.debug(f"\033[32mSFTP-get:remotepath:{remotepath},localpath:{localpath} complete\033[0m")

        elif sftp_mode == "put":
            sftp.put(localpath,remotepath)
            logger.debug(f"\033[32mSFTP-put:localpath:{localpath},remotepath:{remotepath} complete\033[0m")

    except Exception as e:
        logger.error(f"\033[31m{ip} {e}\033[0m")
        ssh_client.close()
        sftp.close()
        return ip, "Failde"

    ssh_client.close()
    sftp.close()
    return ip, "Access"
#========================================================Process========================================================
def Process_run_tasks():
    task_state ={}
    if server_mode == "ssh":
        with ProcessPoolExecutor(max_workers=Max_Process_Workers) as p:
            results = [p.submit(ssh_server, ip) for ip in ip_list]
            for future in as_completed(results):
                task, status = future.result()
                print(f"\033[33mtask {task} \n {status}\033[0m")
                task_state[task] =status
            return task_state

    elif server_mode == "sftp":
        with ProcessPoolExecutor(max_workers=Max_Process_Workers) as p:
            results = [p.submit(sftp_server, ip) for ip in ip_list]
            for future in as_completed(results):
                task, status = future.result()
                task_state[task] =status
            return task_state
    else:
        logger.error(f"\033[33mssh mode [{ssh_mode}] is failed\033[0m")
#========================================================Thread=========================================================
def Thread_run_tasks():
    task_state ={}
    if server_mode == "ssh":
        with ThreadPoolExecutor(max_workers=Max_Thread_Workers) as t:
            results = [t.submit(ssh_server, ip) for ip in ip_list]
            for future in as_completed(results):
                task, status = future.result()
                print(f"\033[33mtask {task} \n {status}\033[0m")
                task_state[task] =status
            return task_state

    elif server_mode == "sftp":
        with ThreadPoolExecutor(max_workers=Max_Thread_Workers) as t:
            results = [t.submit(sftp_server, ip) for ip in ip_list]
            for future in as_completed(results):
                task, status = future.result()
                task_state[task] =status
            return task_state
    else:
        logger.error(f"\033[31mssh mode [{ssh_mode}] is failed\033[0m")
#========================================================Coroutine======================================================
def Coroutine_run_tasks():
    if server_mode == "ssh":
        ctasks = [gevent.spawn(ssh_server, ip) for ip in ip_list]
        gevent.joinall(ctasks)
        for ctask in ctasks:
            task, status = ctask.value
            print(f"\033[33mtask {task} \n {status}\033[0m")
            task_state[task] =status
        return task_state

    elif server_mode == "sftp":
        ctasks = [gevent.spawn(ftp_server, ip) for ip in ip_list]
        gevent.joinall(ctasks)
        for ctask in ctasks:
            task, status = ctask.value
            task_state[task] =status
        return task_state
    else:
        logger.error(f"\033[31mssh mode [{ssh_mode}] is faild\033[0m")
#=======================================================select server===================================================
def select_server(ExecutionMethod):
    global task_state
    task_state = {}
    if ExecutionMethod == "Process":
        task_state = Process_run_tasks()
        return task_state
    elif ExecutionMethod == "Thread":
        task_state = Thread_run_tasks()
        return task_state
    elif  ExecutionMethod == "Coroutine":
        Coroutine_run_tasks()
    else:
        logger.error(f"\033[31mExecutionMethod {ExecutionMethod} is faild\033[0m")
#=======================================================select server===================================================
def select_get_host_method(get_hosts_method):
    if get_hosts_method == "db":
        ip_list = mysql_connect()
        return ip_list
    elif get_hosts_method == "file":
        ip_list = read_file()
        return ip_list
    else:
        logger.error(f"\033[31m{get_hosts_method} is failed\033[0m")
#=======================================================save_to_json_file===================================================
def save_to_json_file():
    result_path = "./result/"
    os.makedirs(result_path, exist_ok=True)
    with open('./result/result.json', result_writing_mode) as file:
        sys.stdout = file
        print(f"time{time.time()}")
        print(result_json)
    sys.stdout = sys.__stdout__
# ======================================================config==================================================================
def read_config(cnf_path):
    with open(cnf_path, 'r') as file:
        config = yaml.safe_load(file)

        return config
#=========================================================main==========================================================
if __name__ == '__main__':

    config = read_config('./conf/conf.yaml')

    log_writing_mode = config['log']['log_writing_mode']
    result_writing_mode = config['log']['result_writing_mode']
    get_hosts_method = config['hosts']['mode']

    hosts_path = config['hosts']['file_conf']['hosts_path']

    db_host = config['hosts']['db_conf']['db_host']
    db_port = config['hosts']['db_conf']['db_port']
    db_user = config['hosts']['db_conf']['db_user']
    db_password = config['hosts']['db_conf']['db_password']
    db_database = config['hosts']['db_conf']['db_database']
    sql = config['hosts']['db_conf']['sql']

    ssh_port = config['ssh']['ssh_port']
    ssh_username = config['ssh']['ssh_username']
    ssh_password = config['ssh']['ssh_password']
    ssh_key_path = config['ssh']['ssh_key_path']

    server_mode = config['tasks']['server_mode']

    commands = config['tasks']['ssh_conf']['commands']


    sftp_mode = config['tasks']['sftp_conf']['sftp_mode']
    localpath = config['tasks']['sftp_conf']['localpath']
    remotepath = config['tasks']['sftp_conf']['remotepath']

    ExecutionMethod = config['tasks']['Execution_method']['mode']
    Max_Process_Workers = config['tasks']['Execution_method']['Max_Process_Workers']
    Max_Thread_Workers = config['tasks']['Execution_method']['Max_Thread_Workers']

    # #============================================下面是启动项 不用改========================================================
    logger = log()
    #logger.debug(f"config: {config}")
    logger.info("\033[33m=====Task Start=====\033[0m")
    ip_list = select_get_host_method(get_hosts_method)


    start_time = time.time()

    task_state = Process_run_tasks()

    logger.info(f"\033[33mTotal hosts: {len(ip_list)}\033[0m")
    logger.info(f"\033[33mTotal time taken: {time.time() - start_time}\033[0m")
    logger.info("\033[33m=====Task End=====\n \033[0m")

    #去掉结果中的换行符 不需要可以注释下面一行代码
    #task_state = {task: state.replace('\n', '') for task, state in task_state.items()}
    result_json = json.dumps(task_state,indent=5)

    save_to_json_file()
