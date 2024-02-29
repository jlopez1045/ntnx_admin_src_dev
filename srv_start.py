#!/usr/bin/python
import os

version = '1.0.0'
print("******************************************************")
print("* Nutanix " + os.path.basename(os.path.realpath(__file__)))
print("* Version " + version)
print("******************************************************")

import sys
import requests
import socket
from subprocess import Popen
from time import sleep

hostname = socket.gethostname()

dir_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(dir_path)


def check_site():
    response = requests.get('http://www.github.com')

    if response.status_code == 200:
        print('======== I am ONLINE')
        return True
    else:
        print('======== I am NOT ONLINE')
        return False


def git_update():
    online = check_site()

    while not online:
        try:
            sleep(15)
            online = check_site()

        except:
            sleep(15)
            online = check_site()

    print('======== Starting Git Pull')

    cmd = 'git pull'
    os.system(cmd)
    sleep(1)

    print('======== Done with Git Pull')


def pip_update():

    print('======== Starting pip install')

    cmd = 'pip install -r requirements.txt'
    os.system(cmd)
    sleep(1)

    print('======== Ending pip install')


if __name__ == "__main__":

    LoopA = True

    # while LoopA:

    try:

        git_update()
        pip_update()

        print(sys.argv[1:])

        cmd = "python3 " + str(dir_path) + "/srv_main.py"

        for x in sys.argv[1:]:
            cmd = cmd + ' ' + str(x)

        print(cmd)

        p1 = Popen(cmd, shell=True)

        poll1 = p1.poll()

        p1.wait()

    except Exception as msg:
        print('======== Failed LoopA', msg)
