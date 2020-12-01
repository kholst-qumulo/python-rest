# this is a demo of how to use requests module to call qumulo API
# it is a demo and example, use for testing, do not run against a production cluster 
# this demo will delete all replications without warning (test2, test3)


import json
import requests
import urllib3
import pickle
import os.path
import time
import logging



# STATIC login info for testing
srcclusterip = '192.168.1.6'
tgtclusterip = '192.168.1.9'
srccluster_user = "admin"
tgtcluster_user = "admin"
srccluster_password = "Password1"
tgtcluster_password = "Password1"


# STATIC config info
timeoutvalue = 3
# default_header = {'content-type': 'application/json'}
urllib3.disable_warnings()
tokenpathname = ""



class Cluster:
    def __init__(self, hostname, token, uuid, user, passwd):
        self.name = hostname
        self.token = token
        self.uuid = uuid
        self.user = user
        self.passwd = passwd

    def login(self):
        global admin_password
        tokenpathname = 'token.pickle.' + str(self.uuid)
        login_json = {"username": self.user, "password": self.passwd}
        logging.debug(login_json)
        base_url = "https://" + self.name + ":8000"
        uri = '/v1/session/login'
        expected_return_code = "200"
        # log into the cluster and obtain a bearer_token
        response = requests.post(
            base_url + uri,
            json=login_json,
            headers=default_header,
            verify=False, timeout=timeoutvalue)
        login_response_code = str(response.status_code)
        logging.debug('Login HTTP Response: ', '\n', response.text)
        token = \
            'Bearer ' + json.loads(response.text)['bearer_token']
        print(response.text)
        if expected_return_code in login_response_code:
            self.token = token
            with open(tokenpathname, 'wb') as tokensave:
                pickle.dump(self.token, tokensave)
                pickle.dump(self.uuid, tokensave)
            return token
        else:
            logging.warning('Login failure: ', response.status_code)
            exit()


def get_json(cluster, uri, expected_response=200, timeout=5):
    url = 'https://{0}:8000{1}'.format(cluster.name, uri)
    ################################################
    # bad workaround for empty token
    ################################################
    if cluster.token == 0:
        default_header = {'content-type': 'application/json'}
    if cluster.token != 0:
        default_header = {'content-type': 'application/json', 'Authorization': cluster.token}
    ################################################
    response = requests.get(url, headers=default_header, verify=False, timeout=timeoutvalue)
    if response.status_code == expected_response:
        return response.json()
    logging.warning('API call failed - status: %s, Body: %s', response.status_code, response.text)


def get_cluster_uuid(cluster):
    uri = '/v1/node/state'
    output = get_json(cluster, uri)
    uuid = str(output['cluster_id'])
    return uuid


def get_cluster_name(cluster):
    uri = '/v1/cluster/settings'
    output = get_json(cluster, uri)
    clustername = str(output['cluster_name'])
    return clustername


def loginhandler(cluster):
    #################################################
    #   open & load a token function                #
    #################################################
    # UUID doesnt need login, get UUID to map to token pickle file
    cluster.uuid = get_cluster_uuid(cluster)
    print('p1 uuid', srccluster.uuid)
    print('p2 uuid', tgtcluster.uuid)

    tokenpathname = 'token.pickle.' + str(cluster.uuid)
    if os.path.exists(tokenpathname):
        st = os.stat(tokenpathname)
        mtime = st.st_mtime
        ts = time.time()
        tokenage = round(((ts - (mtime)) / 3600), 2)
        print('=' * 80)
        print('> Found token for cluster', cluster.uuid)
        print('> Age of token:  {0} hours - expires at 10'.format(tokenage))
        print('=' * 80)
        if tokenage > 0.9:
            print('WARNING: loginhandler: token is older than 9.9 hours')
            cluster.token = cluster.login()  # try to refresh & update token for given uuid
        try:
            with open(tokenpathname, 'rb') as token:
                pickled = pickle.load(token)
                cluster.token = pickled
        except (OSError, Exception) as err:
            logging.warning('ERROR: loginhandler: could not read token.pickle, removing and exiting')
            logging.debug('exception: ', err)
            os.remove(tokenpathname)
            exit()
    else:
        logging.warning('WARN: loginhandler: token not found or expired, logging in with credentials')
        cluster.login()


def read_dir_aggregates(cluster, path):
    uri = '/v1/files/{0}/aggregates/'.format(path)
    output = get_json(cluster, uri)
    return output


def fs_create_dir(cluster, path):
    uri = '/v1/files/{0}/entries/'.format(path)
    output = get_json(cluster, uri)
    return output


def replication_create(srccluster, tgtclusterip, srcpath, tgtpath):
    url = 'https://{0}:8000/v2/replication/source-relationships/'.format(srccluster.name)
    print('replication src cluster debug : token value is : ', srccluster.token)
    default_header = {'content-type': 'application/json', 'Authorization': srccluster.token}
    data = {'target_root_path': srcpath, 'target_address': tgtclusterip, 'source_root_path': tgtpath}
    output = requests.post(url, headers=default_header, json=data, verify=False, timeout=timeoutvalue)
    # print('DEBUG: i did my post call', output.text)
    outputjson = output.json()
    if output.status_code == 400:
        print('DEBUG: ERROR: replication_create: output description: ', outputjson['description'])
        print('DEBUG: ERROR: replication_create: tried to make a replication against a non-existant source-dir')
        exit()

    replicationid = outputjson['id']
    print('DEBUG: demo of how to select new replication id', replicationid)
    return replicationid


def auth_target_replication(cluster, id):
    url = 'https://{0}:8000/v2/replication/target-relationships/{1}/authorize?allow-non-empty-directory=true&allow-fs-path-create=true'.format(cluster.name, id)
    default_header = {'content-type': 'application/json', 'Authorization': cluster.token}
    response = requests.post(url, headers=default_header, verify=False, timeout=timeoutvalue)
    return response


def dismiss_target_replication_error(cluster, id):
    # if you delete a replication from the source
    # need to go to target and dismiss-error
    url = 'https://{0}:8000/v2/replication/target-relationships/{1}/dismiss-error'.format(cluster.name, id)
    print('DEBUG URL :', url)
    default_header = {'content-type': 'application/json', 'Authorization': cluster.token}
    response = requests.post(url, headers=default_header, verify=False, timeout=timeoutvalue)
    print(response)
    return response


def dismiss_source_replication_error(cluster, id):
    # to dismiss a source replication you delete it
    # but only if the target deleted it first
    response = delete_replication_source(cluster, id)
    return response


def list_target_relationship_statuses(cluster):
    uri = '/v2/replication/target-relationships/status/'
    output = get_json(cluster, uri)
    return output


def list_source_relationship_statuses(cluster):
    uri = '/v2/replication/source-relationships/status/'
    output = get_json(cluster, uri)
    return output


def delete_replication_source(cluster, id):
    url = 'https://{0}:8000/v2/replication/source-relationships/{1}'.format(cluster.name, id)
    default_header = {'content-type': 'application/json', 'Authorization': cluster.token}
    response = requests.delete(url, headers=default_header, verify=False, timeout=timeoutvalue)
    print('DEBUG: delete_replication_source: Deleted target id - ', id)
    print(response.text)
    return response


def delete_replication_target(cluster, id):
    # todo : bug workaround - i set requests.delete to requests.post and moved the delete path to URI
    url = 'https://{0}:8000/v2/replication/target-relationships/{1}/delete'.format(cluster.name, id)
    default_header = {'content-type': 'application/json', 'Authorization': cluster.token}
    response = requests.post(url, headers=default_header, verify=False, timeout=timeoutvalue)
    print('DEBUG: delete_replication_target: Deleted target id - ', id)
    print(response.text)
    return response


def get_cluster_network_addresses(cluster, interfaceid, networkid):
    uri = 'GET /v2/network/interfaces/{0}/networks/{1}'.format(interfaceid, networkid)
    output = get_json(cluster, uri)
    return output


def get_source_replication_status(cluster, id):
    uri = '/v2/replication/source-relationships/{0}/status'.format(id)
    output = get_json(cluster, uri)
    # print('DEBUG: get_source_replication_status: output - ', output)
    return output


def get_target_replication_status(cluster,id):
    uri = '/v2/replication/target-relationships/{0}/status'.format(id)
    output = get_json(cluster, uri)
    # print('DEBUG: get_target_replication_status: output - ', output)
    return output


def check_num_active_source_replications():
    result = list_source_relationship_statuses(srccluster)
    resultlength = len(result)
    print('DEBUG: found {0} active source replications'.format(resultlength))
    for itr in range(resultlength):
        currentid = result[itr]['id']
        # print('DEBUG: check_num_active_source_replications', currentid)
    return resultlength


def check_num_active_target_replications():
    # this could take an object instead of re-calling list_target
    result = list_target_relationship_statuses(tgtcluster)
    resultlength = len(result)
    print('DEBUG: check_num_active_target_replications: found {0} active target replications'.format(resultlength))
    for itr in range(resultlength):
        currentid = result[itr]['id']
        # print('DEBUG: check_num_active_target_replications: found', currentid)
    return resultlength


def build_list_of_dirs(cluster, readpath):
    # init empty lists , populate later with subdirs found
    dirnamelist = []

    # Set up SRC info
    basepath_contents = read_dir_aggregates(cluster, readpath)
    basepath_length = len(basepath_contents['files'])

    # Build list of dirs
    print('\n', get_cluster_name(cluster))
    print('DEBUG: build_list_of_dirs: num of dirs found in path {0}: {1}'.format(readpath, basepath_length))
    for itx in range(basepath_length):
        path = basepath_contents['path']
        if basepath_contents['files'][itx]['type'] == 'FS_FILE_TYPE_DIRECTORY':
            dirname = basepath_contents['files'][itx]['name']
            dirnamelist.append(dirname)
            dirsize = basepath_contents['files'][itx]['data_usage']
            print('DEBUG: build_list_of_dirs : basepath {0}, name {1}, size {2},'.format(readpath, dirname, dirsize))
            print('')
    return dirnamelist


def build_list_of_finished_target_replications():
    # only checks target
    # this could be duplicated to check output of single replication call
    # should return list of UUIDs for replication which we know have run and finished
    completedlist = []
    result = list_target_relationship_statuses(tgtcluster)
    resultlength = len(result)
    for itr in range(resultlength):
        duration = result[itr]['duration_of_last_job']
        endreason = result[itr]['end_reason']
        errorfromjob = result[itr]['end_reason']
        currentid = result[itr]['id']
        if duration:
            # making sure duration is not empty
            if not endreason:
                # making sure the end-reason field is empty
                if not errorfromjob:
                    # print('end reason empty, error_from_job empty, duration has a value ')
                    # print(currentid, duration, '\n')
                    completedlist.append(currentid)
                    print('DEBUG: build_list_of_finished_target_replications: added ID - ', currentid)
    return completedlist

def get_list_of_awaiting_target_auth():
    # for test use, find target policies waiting for auth
    waitingforauthlist = []
    result = list_target_relationship_statuses(tgtcluster)
    resultlength = len(result)
    for itr in range(resultlength):
        duration = result[itr]['duration_of_last_job']
        endreason = result[itr]['end_reason']
        errorfromjob = result[itr]['end_reason']
        currentid = result[itr]['id']
        state = result[itr]['state']
        if state == "AWAITING_AUTHORIZATION":
                    waitingforauthlist.append(currentid)
                    print('DEBUG: get_list_of_awaiting_target_auth: build_list_of_target_repls debug - ', currentid)
    return waitingforauthlist











def test1_create_and_destroy_onereplication():
    srcpath = "/1dir"
    tgtpath = "/1dir"
    # testing create, auth, and delete a replication
    # create replication, store new replication UUID
    newreplicationid = replication_create(srccluster, tgtclusterip, srcpath, tgtpath)
    # authorize new replication on target using new replication UUID
    authoutput = auth_target_replication(tgtcluster, newreplicationid)
    # give it time to run
    print('DEBUG: test1_create_and_destroy_onereplication : New replication UUID: {0} \n Auth output JSON: {1}'.format(newreplicationid, authoutput))
    # confirm if the source + target cluster status shows success
    src_status = get_source_replication_status(srccluster, newreplicationid)
    tgt_status = get_target_replication_status(tgtcluster, newreplicationid)
    # todo: check and confirm if replication finished
    time.sleep(3)
    delete_replication_source(srccluster, newreplicationid)
    delete_replication_target(tgtcluster, newreplicationid)
    return

def test2_delete_all_target_relationships():
    # clear all target replcations
    outputfromlistoftargets = list_target_relationship_statuses(tgtcluster)
    qtybeforedelete = len(outputfromlistoftargets)
    for itx in range(qtybeforedelete):
        current_id = outputfromlistoftargets[itx]['id']
        print(current_id)
        delete_replication_target(tgtcluster, current_id)
        dismissoutput = dismiss_target_replication_error(tgtcluster, current_id)
        # print(dismissoutput.text)
        print('deleted \n')

    outputfromlistoftargets = list_target_relationship_statuses(tgtcluster)
    qtyafterdelete = len(outputfromlistoftargets)
    while qtyafterdelete == 0:
        print('DEBUG: PASS: test2_delete_all_target_relationships, {0} src replications found, expected 0'.format(qtyafterdelete))
        return
    else:
        print('DEBUG: ERROR: test2_delete_all_target_relationships: delete qty mismatch - test failed \n before qty-{0} after qty-{1}'.format(qtybeforedelete, qtyafterdelete))
        exit()

def test3_delete_all_source_relationships():
    # clear all target replcations
    outputfromlistallsource = list_source_relationship_statuses(srccluster)
    qtybeforedelete = len(outputfromlistallsource)
    for itx in range(qtybeforedelete):
        current_id = outputfromlistallsource[itx]['id']
        print(current_id)
        delete_replication_source(srccluster, current_id)
        dismissoutput = dismiss_source_replication_error(srccluster, current_id)
        print('DEBUG: test3_delete_all_source_relationships : deleted \n', current_id)
    outputfromlistallsource = list_source_relationship_statuses(srccluster)
    qtyafterdelete = len(outputfromlistallsource)
    while qtyafterdelete == 0:
        print('DEBUG: PASS: test3_delete_all_source_relationships - {0} src replications found, expected 0'.format(qtyafterdelete))
        return
    else:
        print('DEBUG: ERROR: test3_delete_all_source_relationships - test failed \n before qty-{0} after qty-{1}'.format(qtybeforedelete, qtyafterdelete))
        exit()

def test4_compare_tgt_and_src_basepaths():
    # this test should get path and compare
    # on both tgt and src
    # maybe check file size and count too?

    # path we will read from, on source & target cluster
    readpath = "%2F"

    # init empty lists , populate later with subdirs found
    srcnamelist = []
    tgtnamelist = []

    # Set up SRC info
    src_basepath_contents = read_dir_aggregates(srccluster, readpath)
    src_basepath_qty = len(src_basepath_contents['files'])

    # Set up tgt info
    tgt_basepath_contents = read_dir_aggregates(tgtcluster, readpath)
    tgt_basepath_qty = len(tgt_basepath_contents['files'])

    # Build list of source dirs
    # todo - put this into - def build_list_of_dirs()
    print('\n', get_cluster_name(srccluster))
    print('DEBUG: test4_compare_tgt_and_src_basepaths: num of dirs found in src: {0}'.format(src_basepath_qty))
    print('DEBUG: test4_compare_tgt_and_src_basepaths: num of dirs found in tgt: {0}'.format(tgt_basepath_qty))
    for itx in range(src_basepath_qty):
        srcpath = src_basepath_contents['path']
        if src_basepath_contents['files'][itx]['type'] == 'FS_FILE_TYPE_DIRECTORY':
            srcname = src_basepath_contents['files'][itx]['name']
            srcnamelist.append(srcname)
            srcsize = src_basepath_contents['files'][itx]['data_usage']
            print('DEBUG: test4_compare_tgt_and_src_basepaths: basepath {0}, name {1}, size {2},'.format(srcpath, srcname, srcsize))
            print('')

    # Build list of target dirs
    print('\n', get_cluster_name(tgtcluster))
    for itx in range(tgt_basepath_qty):
        tgtpath = tgt_basepath_contents['path']
        if tgt_basepath_contents['files'][itx]['type'] == 'FS_FILE_TYPE_DIRECTORY':
            tgtname = tgt_basepath_contents['files'][itx]['name']
            tgtnamelist.append(tgtname)
            tgtsize = tgt_basepath_contents['files'][itx]['data_usage']
            print('DEBUG: test4_compare_tgt_and_src_basepaths: basepath {0}, name {1}, size {2},'.format(tgtpath, tgtname, tgtsize))

    # check if the files found on target are also in the source list
    # tells us if items in list A are in list B
    # and tells us if items in list B are in list A
    check_tgt_in_src = all(item in tgtnamelist for item in srcnamelist)
    check_src_in_tgt = all(item in srcnamelist for item in tgtnamelist)
    if check_tgt_in_src is True:
        print('DEBUG: test4_compare_tgt_and_src_basepaths: target list contains all of the dir-names found in source list', len(srcnamelist), len(tgtnamelist))
    else:
        print('DEBUG: test4_compare_tgt_and_src_basepaths: target list does not contains all of the dir-names found in source list')
    # check if the files found on source are also in the target list
    if check_src_in_tgt is True:
        print('DEBUG: test4_compare_tgt_and_src_basepaths: source list contains all of the dir-names found in target list', len(srcnamelist), len(tgtnamelist))
    else:
        print('DEBUG: test4_compare_tgt_and_src_basepaths: source list does not contains all of the dir-names found in target list')
    print('DEBUG: test4_compare_tgt_and_src_basepaths: hidden dir names not counted')
    # todo: check if target dirs are empty or not-empty (new replication vs reconnect to old)
    return

def test5_findandcreateall_with_queue():
    readpath = "%2F1kdirs"
    src_basepath = "/1kdirs/"
    tgt_basepath = "/1kdirs/"
    maxqueuedepth = 100
    src_basepath_contents = []

    # check how many total replications are on the tgt
    # these could be complete, running, or errored
    numoftgtreplicationstotal = check_num_active_target_replications()
    print(numoftgtreplicationstotal)

    # Set up SRC info
    src_basepath_contents = build_list_of_dirs(srccluster, readpath)
    print('DEBUG: test5_findandcreateall_with_queue - src basepath ', type(src_basepath_contents))
    src_basepath_qty = len(src_basepath_contents)
    print('DEBUG: test5_findandcreateall_with_queue - num of entries is - ', src_basepath_qty)

    currentqueue = check_num_active_target_replications()

    for entry in src_basepath_contents:
        while currentqueue < maxqueuedepth:
            srcfullpath = src_basepath + entry
            tgtfullpath = tgt_basepath + entry
            newreplicationid = replication_create(srccluster, tgtclusterip, srcfullpath, tgtfullpath)
            #time.sleep(0.5)  #might need delay to slow down timing on virtual test nodes
            auth_target_replication(tgtcluster, newreplicationid)
            currentqueue += 1
            break

        while currentqueue >= maxqueuedepth:
            numoftgtreplicationstotal = check_num_active_target_replications()
            currentqueue = numoftgtreplicationstotal
            finsishedtargetreplications = build_list_of_finished_target_replications()
            for finisheduuid in finsishedtargetreplications:
                delete_replication_target(tgtcluster, finisheduuid)
                currentqueue -= 1
                #time.sleep(0.5)
                dismissoutput = dismiss_source_replication_error(srccluster, finisheduuid)
                print('DEBUG: test5_findandcreateall_with_queue : dismissing source replication error', dismissoutput)
        time.sleep(0.5)

    # cleanup yes need to wait a long time to check, or put this in a try loop and double-check
    print('DEBUG: test5_findandcreateall_with_queue : Complete, sleeping for 10 seconds to wait for API catch up')
    time.sleep(10)
    finishedlist = build_list_of_finished_target_replications()
    for finisheduuid in finishedlist:
        delete_replication_target(tgtcluster, finisheduuid)
        dismissoutput = dismiss_source_replication_error(srccluster, finisheduuid)
        print('DEBUG: test5_findandcreateall_with_queue - cleaning up testrun', dismissoutput)

    return





if __name__ == '__main__':
    # inst the class objects in this layout
    #
    #     hostname      -    token     -   uuid      -    user    -    passwd
    ###############################################################################
    # p1 | 192.168.1.6  |  p1.token    |  p1.uuid    |   admin    |   Password1  |
    # p2 | 192.168.1.9  |  p2.token    |  p2.uuid    |   admin    |   Password1  |
    ##############################################################################
    srccluster = Cluster(srcclusterip, 0, 0, 0, 0)
    tgtcluster = Cluster(tgtclusterip, 0, 0, 0, 0)


    # class obj loaded with user/password here
    srccluster.user = srccluster_user
    srccluster.passwd = srccluster_password
    tgtcluster.user = tgtcluster_user
    tgtcluster.passwd = tgtcluster_password


    # testing login function to both clusters
    print('\nlogin handler Source Cluster')
    loginhandler(srccluster)
    print('\nlogin handler Target Cluster')
    loginhandler(tgtcluster)




    # get target statuses tests/demos
    # test1_create_and_destroy_onereplication()
    # test2_delete_all_target_relationships()
    # test3_delete_all_source_relationships()
    # test4_compare_tgt_and_src_basepaths()
    # test5_findandcreateall_with_queue()
